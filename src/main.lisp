(defpackage #:anypool
  (:nicknames #:anypool/main)
  (:use #:cl)
  (:import-from #:bordeaux-threads)
  (:import-from #:cl-speedy-queue
                #:make-queue
                #:enqueue
                #:dequeue
                #:queue-count
                #:queue-full-p
                #:queue-peek)
  (:export #:*default-max-open-count*
           #:*default-max-idle-count*
           #:pool
           #:make-pool
           #:pool-name
           #:pool-max-open-count
           #:pool-max-idle-count
           #:pool-idle-timeout
           #:pool-max-lifetime
           #:pool-active-count
           #:pool-idle-count
           #:pool-open-count
           #:pool-overflow-count
           #:fetch
           #:putback
           #:too-many-open-connection
           #:error-max-open-limit
           #:with-connection))

(in-package #:anypool)

(defvar *default-max-open-count* 4)
(defvar *default-max-idle-count* 2)

(defun make-queue* (size)
  (if (zerop size)
      (make-array 2 :initial-contents '(2 2))
      (make-queue size)))

(defun queue-peek* (queue)
  (if (= (length queue) 2)
      (values nil nil)
      (queue-peek queue)))

(defstruct (pool (:constructor nil))  ; Don't generate constructor for base struct
  name
  connector
  disconnector
  ping
  max-open-count
  max-idle-count
  unlimited-p
  idle-timeout
  timeout
  ;; Read-only because enabling it on a live pool would leave connections without a creation time.
  (max-lifetime nil :read-only t)
  ;; Internal
  storage ; queue object
  (active-count 0 :type fixnum)
  (lock (bt2:make-lock :name "ANYPOOL-LOCK"))
  (wait-lock (bt2:make-lock :name "ANYPOOL-OPENWAIT-LOCK"))
  (wait-condvar (bt2:make-condition-variable :name "ANYPOOL-OPENWAIT"))
  (lifetime-limit nil :read-only t) ; max-lifetime * internal-time-units-per-second, exact
  ;; Connection -> creation time. Kept apart from ITEM, which lives for one idle period only.
  (created-at-table nil :read-only t)
  (clock #'get-internal-real-time))

(defstruct (pool-without-timeout
            (:include pool)
            (:conc-name pool-)
            (:constructor %make-pool-without-timeout)))

(defstruct (pool-with-timeout
            (:include pool)
            (:conc-name pool-)
            (:constructor %make-pool-with-timeout))
  (timeout-in-queue-count 0 :type fixnum))

(defstruct (item (:constructor make-item (object)))
  object
  idle-timer
  (active-p nil)
  (timeout-p nil))

(defmethod print-object ((object pool) stream)
  (print-unreadable-object (object stream :type t :identity t)
    (format stream "~@[~S ~](OPEN=~A / IDLE=~A)"
            (pool-name object)
            (pool-open-count object)
            (pool-idle-count object))))

(defun pool-idle-count (pool)
  (etypecase pool
    (pool-without-timeout
     (queue-count (pool-storage pool)))
    (pool-with-timeout
     (- (queue-count (pool-storage pool))
        (pool-timeout-in-queue-count pool)))))

(defun pool-open-count (pool)
  (+ (pool-active-count pool)
     (pool-idle-count pool)))

(defun pool-overflow-count (pool)
  "Number of connections beyond max-open-count (0 if unlimited)."
  (if (pool-unlimited-p pool)
      0
      (max 0 (- (pool-active-count pool)
                (pool-max-open-count pool)))))

(define-condition anypool-error (error) ())
(define-condition too-many-open-connection (anypool-error)
  ((limit :initarg :limit
          :reader error-max-open-limit))
  (:report (lambda (condition stream)
             (format stream "Too many open connection and couldn't open a new one (limit: ~A)"
                     (slot-value condition 'limit)))))

(define-condition resource-already-in-pool (anypool-error)
  ((resource :initarg :resource))
  (:report (lambda (condition stream)
             (format stream "The connector returned ~S, which the pool already manages. ~
                             With max-lifetime, it must return a new object on every call."
                     (slot-value condition 'resource)))))

(defun lifetime-limit (max-lifetime)
  "Return MAX-LIFETIME milliseconds times internal-time-units-per-second as an exact rational.
Comparing it with 1000 times the elapsed internal time decides expiry without rounding."
  (or (and (typep max-lifetime '(real (0)))
           ;; RATIONAL signals on a float infinity.
           (ignore-errors (* (rational max-lifetime) internal-time-units-per-second)))
      (error 'type-error :datum max-lifetime :expected-type '(or null (real (0))))))

(defun make-pool (&key name
                       connector
                       disconnector
                       (ping (constantly t))
                       (max-open-count *default-max-open-count*)
                       (max-idle-count *default-max-idle-count*)
                       timeout
                       idle-timeout
                       max-lifetime)
  "Create a connection pool. Returns pool-without-timeout or pool-with-timeout based on idle-timeout."
  (let* ((unlimited-p (null max-open-count))
         (max-open-count (or max-open-count most-positive-fixnum))
         (storage (make-queue* (min max-open-count max-idle-count)))
         (lifetime-limit (and max-lifetime (lifetime-limit max-lifetime)))
         (created-at-table (and max-lifetime (make-hash-table :test 'eql))))
    (if idle-timeout
        (%make-pool-with-timeout
         :name name
         :connector connector
         :disconnector disconnector
         :ping ping
         :max-open-count max-open-count
         :max-idle-count max-idle-count
         :unlimited-p unlimited-p
         :idle-timeout idle-timeout
         :timeout timeout
         :max-lifetime max-lifetime
         :lifetime-limit lifetime-limit
         :created-at-table created-at-table
         :storage storage)
        (%make-pool-without-timeout
         :name name
         :connector connector
         :disconnector disconnector
         :ping ping
         :max-open-count max-open-count
         :max-idle-count max-idle-count
         :unlimited-p unlimited-p
         :idle-timeout nil
         :timeout timeout
         :max-lifetime max-lifetime
         :lifetime-limit lifetime-limit
         :created-at-table created-at-table
         :storage storage))))

(defun forget-created-at (pool conn)
  (let ((table (pool-created-at-table pool)))
    (when table
      (remhash conn table))))

(defun register-created-at (pool conn)
  "Record now as the creation time of CONN, which the connector has just returned."
  (let ((table (pool-created-at-table pool)))
    ;; Overwriting would silently restart the lifetime of a connection that may be lent out.
    (when (nth-value 1 (gethash conn table))
      (error 'resource-already-in-pool :resource conn))
    (setf (gethash conn table) (funcall (pool-clock pool)))))

(defun expired-p (pool conn)
  "Whether CONN has reached max-lifetime. The clock is not read when max-lifetime is disabled.
A CONN without a creation time, i.e. not from this pool's connector, counts as expired."
  (when (pool-max-lifetime pool)
    (let ((created-at (gethash conn (pool-created-at-table pool))))
      (or (null created-at)
          (>= (* 1000 (- (funcall (pool-clock pool)) created-at))
              (pool-lifetime-limit pool))))))

(defun lendable-p (pool conn)
  "With the pool lock held, check an idle CONN before lending it; a rejected CONN is dropped.
Lifetime is checked again after PING, which may take long enough for CONN to expire."
  (let ((ping (pool-ping pool))
        (verdict nil))
    (unwind-protect
         (setf verdict
               (cond ((expired-p pool conn) :expired)
                     ((and ping (not (funcall ping conn))) :ping-failed)
                     ((expired-p pool conn) :expired)
                     (t :lendable)))
      ;; Also when PING signals: CONN has already left the queue and won't come back.
      (unless (eq verdict :lendable)
        (forget-created-at pool conn)))
    (or (eq verdict :lendable)
        ;; Expired or ping failed, disconnect and continue
        (let ((disconnector (pool-disconnector pool)))
          (when disconnector
            (ignore-errors (funcall disconnector conn)))
          nil))))

(defmacro define-fetch-impl (name pool-type &key idle-check dequeue-and-validate)
  "Generate a fetch implementation with type-specific logic."
  `(defun ,name (pool)
     (declare (type ,pool-type pool))
     (with-slots (connector disconnector ping storage lock timeout unlimited-p wait-lock wait-condvar) pool
       (flet ((allocate-new ()
                (let ((conn (funcall connector)))
                  (when (pool-max-lifetime pool)
                    (register-created-at pool conn))
                  conn))
              (can-open-p ()
                (or unlimited-p
                    (< (pool-open-count pool) (pool-max-open-count pool)))))
         (declare (inline allocate-new))
         (loop
           (bt2:with-lock-held (lock)
             (if ,idle-check
                 ;; Common wait-or-allocate logic
                 (cond
                   ((can-open-p)
                    (return (prog1 (allocate-new)
                              (incf (pool-active-count pool)))))
                   ((and (numberp timeout)
                         (zerop timeout))
                    (error 'too-many-open-connection
                           :limit (pool-max-open-count pool)))
                   (t
                    (unwind-protect
                        (or #+ccl
                            (progn
                              (bt2:release-lock lock)
                              (if timeout
                                  (ccl:timed-wait-on-semaphore wait-condvar (/ timeout 1000d0))
                                  (ccl:wait-on-semaphore wait-condvar)))
                            #-ccl
                            ;; WAIT-LOCK is taken before LOCK is released: putback notifies under
                            ;; WAIT-LOCK, so its wakeup cannot fall between our check and the wait.
                            (bt2:with-lock-held (wait-lock)
                              (bt2:release-lock lock)
                              (bt2:condition-wait wait-condvar wait-lock
                                                  :timeout (and timeout (/ timeout 1000d0))))
                            (error 'too-many-open-connection
                                   :limit (pool-max-open-count pool)))
                      (bt2:acquire-lock lock))))
                 ;; Type-specific dequeue and validation
                 ,dequeue-and-validate)))))))

(define-fetch-impl %fetch-without-timeout pool-without-timeout
  :idle-check (zerop (queue-count storage))
  :dequeue-and-validate
  (let ((conn (dequeue storage)))
    (when (lendable-p pool conn)
      (incf (pool-active-count pool))
      (return conn))))

#+sbcl
(defun make-idle-timer (item timeout-fn)
  (sb-ext:make-timer
    (lambda ()
      (unless (item-timeout-p item)
        (funcall timeout-fn (item-object item))))
    :thread t))

(define-fetch-impl %fetch-with-timeout pool-with-timeout
  :idle-check (<= (pool-idle-count pool) 0) ; Fail-safe for cases where pool-idle-count is negative
  :dequeue-and-validate
  (let ((item (dequeue storage)))
    (if (item-timeout-p item)
        (decf (pool-timeout-in-queue-count pool))
        (let ((lent nil))
          ;; Counted as active while LOCK is released below, or a concurrent fetch would
          ;; see a free slot and open past max-open-count.
          (incf (pool-active-count pool))
          ;; The idle timer checks this under LOCK, so from here on it leaves ITEM alone.
          (setf (item-active-p item) t)
          (unwind-protect
               (progn
                 #+sbcl
                 (when (item-idle-timer item)
                   ;; Release the lock once to prevent from deadlock
                   (bt2:release-lock lock)
                   (unwind-protect (sb-ext:unschedule-timer (item-idle-timer item))
                     (bt2:acquire-lock lock)))
                 (setf lent (lendable-p pool (item-object item))))
            (unless lent
              (decf (pool-active-count pool))))
          (when lent
            (return (item-object item)))))))

(defun fetch (pool)
  "Fetch a connection from the pool."
  (etypecase pool
    (pool-without-timeout (%fetch-without-timeout pool))
    (pool-with-timeout (%fetch-with-timeout pool))))

(defun notify-waiter (pool)
  (bt2:with-lock-held ((pool-wait-lock pool))
    (bt2:condition-notify (pool-wait-condvar pool))))

(defun call-then-release-slot (pool fn)
  "Call FN, then free the slot of a lent-out connection and wake a waiter even if FN fails.
The slot is held until FN returns so a waiter cannot open a replacement while the old one is still open."
  (unwind-protect (funcall fn)
    (bt2:with-lock-held ((pool-lock pool))
      (decf (pool-active-count pool)))
    (notify-waiter pool)))

(defmacro define-putback-impl (name pool-type &key before-putback enqueue-logic)
  "Generate a putback implementation with type-specific logic."
  `(defun ,name (conn pool)
     (declare (type ,pool-type pool))
     ,@(when before-putback (list before-putback))
     (with-slots (disconnector storage lock) pool
       (ecase (bt2:with-lock-held (lock)
                (cond
                  ((expired-p pool conn)
                   (forget-created-at pool conn)
                   :expired)
                  ((queue-full-p storage)
                   (forget-created-at pool conn)
                   :full)
                  (t
                   ,enqueue-logic
                   (decf (pool-active-count pool))
                   :enqueued)))
         (:enqueued
          (notify-waiter pool))
         (:expired
          ;; Errors are ignored as for a failed ping; the connection is gone either way.
          (call-then-release-slot pool
                                  (lambda ()
                                    (when disconnector
                                      (ignore-errors (funcall disconnector conn))))))
         (:full
          (call-then-release-slot pool
                                  (lambda ()
                                    (when disconnector
                                      (funcall disconnector conn)))))))
     (values)))

(defun dequeue-timeout-resources (pool)
  (declare (type pool-with-timeout pool))
  (with-slots (storage lock) pool
    (loop
      (bt2:with-lock-held (lock)
        (let ((item (queue-peek* storage)))
          (when (or (null item)
                    (not (item-timeout-p item)))
            (return))
          (decf (pool-timeout-in-queue-count pool))
          (dequeue storage))))))

(define-putback-impl %putback-without-timeout pool-without-timeout
  :enqueue-logic
  (enqueue conn storage))

(define-putback-impl %putback-with-timeout pool-with-timeout
  :before-putback
  (dequeue-timeout-resources pool)
  :enqueue-logic
  (let ((item (make-item conn)))
    #+sbcl
    (with-slots (idle-timeout) pool
      (setf (item-idle-timer item)
            (make-idle-timer item
                             (lambda (conn)
                               (let ((activep
                                       (bt2:with-lock-held (lock)
                                         (let ((activep (item-active-p item)))
                                           (unless activep
                                             (setf (item-timeout-p item) t)
                                             (incf (pool-timeout-in-queue-count pool))
                                             (forget-created-at pool conn))
                                           activep))))
                                 (unless activep
                                   (when disconnector
                                     (funcall disconnector conn)))))))
      (sb-ext:schedule-timer (item-idle-timer item)
                             (/ idle-timeout 1000d0)))
    (enqueue item storage)))

(defun putback (conn pool)
  "Return a connection to the pool."
  (etypecase pool
    (pool-without-timeout (%putback-without-timeout conn pool))
    (pool-with-timeout (%putback-with-timeout conn pool))))

(defmacro with-connection ((conn pool) &body body)
  (let ((g-pool (gensym "POOL")))
    `(let* ((,g-pool ,pool)
            (,conn (fetch ,g-pool)))
       (unwind-protect (progn ,@body)
         (putback ,conn ,g-pool)))))
