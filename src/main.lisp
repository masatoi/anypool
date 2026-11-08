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
           #:failed-disconnection
           #:failed-disconnection-connection
           #:failed-disconnection-condition
           #:pool-failed-disconnections
           #:acknowledge-failed-disconnection
           #:fetch
           #:putback
           #:too-many-open-connection
           #:connection-expired-before-use
           #:error-max-open-limit
           #:with-connection))

(in-package #:anypool)

(defvar *default-max-open-count* 4)
(defvar *default-max-idle-count* 2)

;;; SB-EXT:WITH-TIMEOUT and INTERRUPT-THREAD can unwind a thread between any two forms. Slot
;;; accounting runs with them deferred so a slot is never lost or freed twice. Only SBCL is
;;; protected; elsewhere these are PROGN.

(defmacro without-interrupts* (&body body)
  #+sbcl `(sb-sys:without-interrupts ,@body)
  #-sbcl `(progn ,@body))

(defmacro with-local-interrupts* (&body body)
  "Allow interrupts in BODY again. Must appear lexically inside WITHOUT-INTERRUPTS*."
  #+sbcl `(sb-sys:with-local-interrupts ,@body)
  #-sbcl `(progn ,@body))

(defmacro allow-with-interrupts* (&body body)
  "Keep interrupts deferred in BODY, but let a WITH-LOCAL-INTERRUPTS* further in allow them.
Must appear lexically inside WITHOUT-INTERRUPTS*."
  #+sbcl `(sb-sys:allow-with-interrupts ,@body)
  #-sbcl `(progn ,@body))

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
  ;; Track identities even without max-lifetime, so cleanup does not close an object that an
  ;; interrupted connector returned while another borrower already owns it.
  (managed-table (make-hash-table :test 'eql) :read-only t)
  (retiring-table (make-hash-table :test 'eql) :read-only t)
  (failed-disconnection-records nil)
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

(defstruct (failed-disconnection
            (:constructor %make-failed-disconnection (connection condition)))
  (connection nil :read-only t)
  (condition nil :read-only t))

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
             (format stream "The resource ~S is already managed or being disconnected."
                     (slot-value condition 'resource)))))

(define-condition connection-expired-before-use (anypool-error) ()
  (:report (lambda (condition stream)
             (declare (ignore condition))
             (write-string "The new connection expired before it could be lent." stream))))

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
      (remhash conn table)))
  (let* ((table (pool-managed-table pool))
         (count (gethash conn table)))
    (when count
      (if (= count 1)
          (remhash conn table)
          (setf (gethash conn table) (1- count))))))

(defun register-created-at (pool conn created-at)
  "Record CREATED-AT, captured immediately after the connector returned CONN."
  (let ((table (pool-created-at-table pool)))
    ;; Overwriting would silently restart the lifetime of a connection that may be lent out.
    (when (nth-value 1 (gethash conn table))
      (error 'resource-already-in-pool :resource conn))
    (setf (gethash conn table) created-at)))

(defun register-managed (pool conn)
  "With the pool lock held, record one slot owning CONN."
  (when (gethash conn (pool-retiring-table pool))
    (error 'resource-already-in-pool :resource conn))
  (incf (gethash conn (pool-managed-table pool) 0)))

(defun prepare-retirement (pool conn release-slot-p invalidate-shared-p)
  "With the pool lock held, drop a shared slot or claim the last reference for closing.
An invalid shared identity is never lent again, but is closed only after its last slot retires."
  (let* ((retiring (pool-retiring-table pool))
         (state (gethash conn retiring))
         (references (gethash conn (pool-managed-table pool) 0)))
    (cond
      ((member state '(:closing :failed))
       :already-retiring)
      ((> references 1)
       (when (or invalidate-shared-p (eq state :invalid))
         (setf (gethash conn retiring) :invalid))
       (forget-created-at pool conn)
       (when release-slot-p
         (decf (pool-active-count pool)))
       :released)
      (t
       (setf (gethash conn retiring) :closing)
       :close))))

(defun unmark-retiring (pool conn)
  "With the pool lock held, allow CONN's identity to be registered again."
  (remhash conn (pool-retiring-table pool)))

(defun created-at (pool conn)
  "With the pool lock held, return the creation time recorded for CONN, or NIL."
  (let ((table (pool-created-at-table pool)))
    (and table (gethash conn table))))

(defun expired-at-p (pool created-at)
  "Whether a connection created at CREATED-AT has reached max-lifetime. The clock is not read when
max-lifetime is disabled. A NIL CREATED-AT, i.e. not from this pool's connector, counts as expired."
  (when (pool-max-lifetime pool)
    (or (null created-at)
        (>= (* 1000 (- (funcall (pool-clock pool)) created-at))
            (pool-lifetime-limit pool)))))

(defun expired-p (pool conn)
  "With the pool lock held, whether CONN has reached max-lifetime."
  (and (pool-max-lifetime pool)
       (expired-at-p pool (created-at pool conn))))

(defun lendable-p (pool conn created-at)
  "Without the pool lock, check an idle CONN that fetch has taken out. Retiring a rejected CONN is up
to the caller. Lifetime is checked again after PING, which may take long enough for CONN to expire."
  (let ((ping (pool-ping pool)))
    (and (not (expired-at-p pool created-at))
         (or (null ping) (funcall ping conn))
         (not (expired-at-p pool created-at)))))

#+sbcl
(defun make-idle-timer (item timeout-fn)
  (sb-ext:make-timer
    (lambda ()
      (unless (item-timeout-p item)
        (funcall timeout-fn (item-object item))))
    :thread t))

(defun take-idle (pool)
  "With the pool lock held, dequeue an idle connection and return it with its creation time and
queue item. The fourth value says whether a connection was found, even when that connection is NIL.
The fifth value is its retirement state. A dequeued item is marked active so its timer leaves it alone."
  (etypecase pool
    (pool-without-timeout
     (let ((storage (pool-storage pool)))
       (unless (zerop (queue-count storage))
         (let ((conn (dequeue storage)))
           (values conn (created-at pool conn) nil t
                   (gethash conn (pool-retiring-table pool)))))))
    (pool-with-timeout
     (loop
       ;; Fail-safe for cases where pool-idle-count is negative
       (when (<= (pool-idle-count pool) 0)
         (return nil))
       (let ((item (dequeue (pool-storage pool))))
         (if (item-timeout-p item)
             (decf (pool-timeout-in-queue-count pool))
             (let ((conn (item-object item)))
               (setf (item-active-p item) t)
               (return (values conn (created-at pool conn) item t
                               (gethash conn (pool-retiring-table pool)))))))))))

(defun can-open-p (pool)
  (or (pool-unlimited-p pool)
      (< (pool-open-count pool) (pool-max-open-count pool))))

(defun wait-for-slot (pool)
  "With the pool lock held, wait until a slot may have been freed; the lock is held again on return.
Return NIL when TIMEOUT runs out, so the caller can signal after releasing the lock."
  (with-slots (lock timeout wait-lock wait-condvar) pool
    (unless (and (numberp timeout)
                 (zerop timeout))
      ;; SBCL's mutex release and grab are not interrupt-safe: interrupted halfway, LOCK is left
      ;; taken by no thread and every later fetch hangs. Interrupts are allowed only while sleeping.
      (without-interrupts*
        (unwind-protect
             #+ccl
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
               (allow-with-interrupts*
                 (bt2:condition-wait wait-condvar wait-lock
                                     :timeout (and timeout (/ timeout 1000d0)))))
          (allow-with-interrupts* (bt2:acquire-lock lock)))))))

(defun release-idle-timer (item)
  "Stop the idle timer of ITEM, which fetch has taken out. Called without the pool lock, which the
timer callback takes."
  #+sbcl
  (when (and item (item-idle-timer item))
    (sb-ext:unschedule-timer (item-idle-timer item)))
  #-sbcl
  (declare (ignore item)))

(defun fetch (pool)
  "Fetch a connection from the pool.
PING and CONNECTOR run without the pool lock, so a slow or stuck one delays only this call. The slot
they work in is counted as active meanwhile, so the pool never opens more than max-open-count."
  (let ((lock (pool-lock pool))
        (held nil)                      ; this call owns a slot counted in ACTIVE-COUNT
        (owned nil)                     ; a connection only this call holds
        (owned-p nil)
        (registered-p nil)
        (lent nil)
        (lent-p nil))
    (without-interrupts*
      (unwind-protect
           (progn
             (with-local-interrupts*
               (loop
                 (let ((action nil)
                       (created-at nil)
                       (item nil)
                       (retiring-p nil))
                   (bt2:with-lock-held (lock)
                     (loop
                       ;; Counts change together with HELD and OWNED; an interrupt in between
                       ;; would leak the slot or free it twice.
                       (without-interrupts*
                         (multiple-value-bind (conn conn-created-at conn-item present-p conn-retiring-p)
                             (take-idle pool)
                           (cond
                             (present-p
                              (unless held
                                (incf (pool-active-count pool))
                                (setf held t))
                              (setf owned conn
                                    owned-p t
                                    registered-p t
                                    created-at conn-created-at
                                    item conn-item
                                    retiring-p conn-retiring-p
                                    action :check))
                             ;; A slot kept after retiring a connection is used, never waited
                             ;; with, or every slot could end up held by a waiter.
                             ((or held (can-open-p pool))
                              (unless held
                                (incf (pool-active-count pool))
                                (setf held t))
                              (setf action :open)))))
                       (when action
                         (return))
                       (unless (wait-for-slot pool)
                         (return))))
                   ;; Signalled outside the lock: handlers may do I/O, and every fetch and putback
                   ;; would wait for them.
                   (unless action
                     (error 'too-many-open-connection
                            :limit (pool-max-open-count pool)))
                   (ecase action
                     (:check
                      (release-idle-timer item)
                      (when (and (not retiring-p)
                                 (lendable-p pool owned created-at)
                                 (bt2:with-lock-held (lock)
                                   (and (not (gethash owned (pool-retiring-table pool)))
                                        (not (expired-at-p pool created-at)))))
                        (return))
                      ;; Transfer the connection to retirement atomically. If the close fails or
                      ;; is interrupted, the retirement record takes over this slot.
                      (let ((conn owned)
                            (retired-p nil)
                            (retirement nil))
                        (without-interrupts*
                          (bt2:with-lock-held (lock)
                            (setf retirement (prepare-retirement pool conn nil t))
                            (setf owned nil
                                  owned-p nil
                                  registered-p nil))
                          (ecase retirement
                            (:released nil)
                            (:close
                             (unwind-protect
                                  (progn
                                    (allow-with-interrupts* (retire-connection pool conn nil))
                                    (setf retired-p t))
                               (unless retired-p
                                 (setf held nil))))
                            (:already-retiring
                             (setf held nil)
                             (error 'resource-already-in-pool :resource conn))))))
                     (:open
                      ;; Capture the result before waiting for the pool lock. The connector remains
                      ;; interruptible, but ownership and the timestamp are recorded atomically.
                      (without-interrupts*
                        (setf owned (with-local-interrupts* (funcall (pool-connector pool)))
                              owned-p t)
                        (when (pool-max-lifetime pool)
                          (setf created-at (funcall (pool-clock pool)))))
                      (bt2:with-lock-held (lock)
                        (without-interrupts*
                          (when (pool-max-lifetime pool)
                            (register-created-at pool owned created-at))
                          (register-managed pool owned)
                          (setf registered-p t)))
                      (when (expired-at-p pool created-at)
                        (error 'connection-expired-before-use))
                      (return))))))
             ;; The connection is safe from interrupts until FETCH returns; past that the caller
             ;; must defer them, as WITH-CONNECTION does.
             (setf lent owned
                   lent-p owned-p
                   owned nil
                   owned-p nil))
        (unless lent-p
          ;; Preserve the original error when cleanup itself fails, but retain the connection and
          ;; its slot in a failure record. Non-local exits from cleanup still take effect.
          (unwind-protect
               (when owned-p
                 (let ((conn owned)
                       (already-managed-p nil)
                       (retirement nil))
                   (bt2:with-lock-held (lock)
                     ;; A connector can return an object already lent elsewhere. An interrupt or
                     ;; duplicate-registration error must not disconnect that other borrower's object.
                     (setf already-managed-p
                           (and (not registered-p)
                                (nth-value 1 (gethash conn (pool-managed-table pool)))))
                     (unless (or registered-p already-managed-p)
                       ;; The connector returned before registration. Claim its identity so a
                       ;; failed cleanup remains visible and cannot be lent a second time.
                       (when (pool-max-lifetime pool)
                         (register-created-at pool conn 0))
                       (register-managed pool conn))
                     (unless already-managed-p
                       (setf retirement (prepare-retirement pool conn t t)))
                     (setf owned nil
                           owned-p nil
                           registered-p nil))
                   (unless already-managed-p
                     (setf held nil)
                     (ecase retirement
                       (:released (notify-waiter pool))
                       (:close
                        (ignore-errors
                          (allow-with-interrupts* (retire-connection pool conn t))))
                       (:already-retiring nil)))))
            (when held
              (release-slot pool))))))
    lent))

(defun notify-waiter (pool)
  (bt2:with-lock-held ((pool-wait-lock pool))
    (bt2:condition-notify (pool-wait-condvar pool))))

(defun release-slot (pool)
  "Free one slot counted in ACTIVE-COUNT and wake a waiter to claim it."
  (without-interrupts*
    (bt2:with-lock-held ((pool-lock pool))
      (decf (pool-active-count pool)))
    (notify-waiter pool)))

(defun pool-failed-disconnections (pool)
  "Return a snapshot of failed disconnections that still occupy pool capacity."
  (bt2:with-lock-held ((pool-lock pool))
    (copy-list (pool-failed-disconnection-records pool))))

(defun acknowledge-failed-disconnection (pool record)
  "Release RECORD's slot after the caller has verified that its connection is closed.
Return true only when RECORD was still awaiting acknowledgment in POOL."
  (let ((acknowledged nil))
    (without-interrupts*
      (bt2:with-lock-held ((pool-lock pool))
        (when (member record (pool-failed-disconnection-records pool) :test #'eq)
          (setf (pool-failed-disconnection-records pool)
                (remove record (pool-failed-disconnection-records pool) :test #'eq :count 1))
          (forget-created-at pool (failed-disconnection-connection record))
          (unmark-retiring pool (failed-disconnection-connection record))
          (decf (pool-active-count pool))
          (setf acknowledged t)))
      (when acknowledged
        (notify-waiter pool)))
    acknowledged))

(defun retire-connection (pool conn release-slot-p)
  "Disconnect CONN outside the lock. Retain its slot and identity on any abnormal exit.
When RELEASE-SLOT-P is NIL, a successful caller keeps the slot for a replacement."
  (let ((completed nil)
        (failure nil))
    (without-interrupts*
      (unwind-protect
           (progn
             (with-local-interrupts*
               (handler-bind ((error (lambda (condition) (setf failure condition))))
                 (let ((disconnector (pool-disconnector pool)))
                   (when disconnector
                     (funcall disconnector conn)))))
             (setf completed t))
        (bt2:with-lock-held ((pool-lock pool))
          (if completed
              (progn
                (forget-created-at pool conn)
                (unmark-retiring pool conn)
                (when release-slot-p
                  (decf (pool-active-count pool))))
              (progn
                (setf (gethash conn (pool-retiring-table pool)) :failed)
                (push (%make-failed-disconnection conn failure)
                      (pool-failed-disconnection-records pool)))))
        (when (and completed release-slot-p)
          (notify-waiter pool))))))

(defmacro define-putback-impl (name pool-type &key before-putback enqueue-logic)
  "Generate a putback implementation with type-specific logic."
  `(defun ,name (conn pool)
     (declare (type ,pool-type pool))
     ;; Interrupted between the decision and its outcome, CONN would be neither queued nor its
     ;; slot freed. Only the disconnect, which does I/O, can be interrupted.
     (without-interrupts*
       ,@(when before-putback (list before-putback))
       (with-slots (storage lock) pool
         (ecase (bt2:with-lock-held (lock)
                  (cond
                    ((member (gethash conn (pool-retiring-table pool)) '(:closing :failed))
                     :already-retiring)
                    ((eq (gethash conn (pool-retiring-table pool)) :invalid)
                     (prepare-retirement pool conn t t))
                    ((expired-p pool conn)
                     (prepare-retirement pool conn t t))
                    ((queue-full-p storage)
                     (prepare-retirement pool conn t nil))
                    (t
                     ,enqueue-logic
                     (decf (pool-active-count pool))
                     :enqueued)))
           (:enqueued
            (notify-waiter pool))
           (:released
            (notify-waiter pool))
           (:close
            (allow-with-interrupts* (retire-connection pool conn t)))
           (:already-retiring
            (error 'resource-already-in-pool :resource conn)))))
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
                               (without-interrupts*
                                 (multiple-value-bind (activep retirement)
                                     (bt2:with-lock-held (lock)
                                       (if (item-active-p item)
                                           (values t nil)
                                           (progn
                                             (setf (item-timeout-p item) t)
                                             (incf (pool-timeout-in-queue-count pool))
                                             ;; Count the dequeued slot until it is retired.
                                             (incf (pool-active-count pool))
                                             (values nil (prepare-retirement pool conn t nil)))))
                                   (unless activep
                                     ;; A timer has no caller to receive an error; retain it in the
                                     ;; pool's failed-disconnection records instead.
                                     (ecase retirement
                                       (:released (notify-waiter pool))
                                       (:close
                                        (ignore-errors
                                          (allow-with-interrupts*
                                            (retire-connection pool conn t))))
                                       (:already-retiring nil))))))))
      (sb-ext:schedule-timer (item-idle-timer item)
                             (/ idle-timeout 1000d0)))
    (enqueue item storage)))

(defun putback (conn pool)
  "Return a connection to the pool."
  (etypecase pool
    (pool-without-timeout (%putback-without-timeout conn pool))
    (pool-with-timeout (%putback-with-timeout conn pool))))

(defmacro with-connection ((conn pool) &body body)
  (let ((g-pool (gensym "POOL"))
        (g-bound (gensym "BOUND"))
        (g-completed (gensym "COMPLETED")))
    ;; FETCH/PUTBACK go through ALLOW-WITH-INTERRUPTS*, not WITH-LOCAL-INTERRUPTS*: their own
    ;; WITHOUT-INTERRUPTS* would otherwise deliver a pending interrupt before CONN is bound.
    `(let ((,g-pool ,pool)
           (,conn nil)
           (,g-bound nil)
           (,g-completed nil))
       (without-interrupts*
         (unwind-protect
              (progn
                (setf ,conn (allow-with-interrupts* (fetch ,g-pool))
                      ,g-bound t)
                (multiple-value-prog1 (with-local-interrupts* ,@body)
                  (setf ,g-completed t)))
           (when ,g-bound
             (if ,g-completed
                 (allow-with-interrupts* (putback ,conn ,g-pool))
                 ;; Keep the body's non-local exit when cleanup signals an error; any failed
                 ;; disconnection is still recorded for later inspection.
                 (ignore-errors
                   (allow-with-interrupts* (putback ,conn ,g-pool))))))))))
