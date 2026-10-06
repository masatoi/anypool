(defpackage #:anypool/tests/io-outside-lock
  (:use #:cl
        #:rove
        #:anypool)
  (:import-from #:anypool
                #:pool-clock
                #:pool-created-at-table
                #:pool-lock
                #:without-interrupts*
                #:with-local-interrupts*)
  (:import-from #:anypool/tests/utils
                #:start-waiting-fetch))
(in-package #:anypool/tests/io-outside-lock)

;;; A gate stops a callback (connector, ping, disconnector) at a known point, so a test can act on
;;; the pool while that callback is in progress.
(defstruct (gate (:constructor make-gate ()))
  (entered (bt2:make-semaphore :name "gate entered"))
  (release (bt2:make-semaphore :name "gate release")))

(defun pass-gate (gate)
  (bt2:signal-semaphore (gate-entered gate))
  (bt2:wait-on-semaphore (gate-release gate)))

(defun await-gate (gate)
  (or (bt2:wait-on-semaphore (gate-entered gate) :timeout 5)
      (error "Nothing reached the gate")))

(defun open-gate (gate &optional (count 1))
  (bt2:signal-semaphore (gate-release gate) :count count))

(defun make-counter ()
  (let ((n 0)
        (lock (bt2:make-lock :name "counter")))
    (lambda ()
      (bt2:with-lock-held (lock)
        (incf n)))))

(defun spawn (fn)
  (bt2:make-thread (lambda ()
                     (handler-case (list :ok (funcall fn))
                       (error (e) (list :error e))))))

(defun join-within (thread seconds)
  (let ((deadline (+ (get-internal-real-time)
                     (* seconds internal-time-units-per-second))))
    (loop
      (unless (bt2:thread-alive-p thread)
        (return (values t (bt2:join-thread thread))))
      (when (> (get-internal-real-time) deadline)
        (return (values nil nil)))
      (sleep 0.01))))

(defun call-within (seconds fn)
  (multiple-value-bind (finished result) (join-within (spawn fn) seconds)
    (values finished
            (and finished
                 (if (eq (first result) :ok)
                     (second result)
                     (error (second result)))))))

(define-condition interrupted (error) ())

(defun interrupt (thread)
  (bt2:interrupt-thread thread (lambda () (error 'interrupted))))

(defun interrupted-p (result)
  (and (eq (first result) :error)
       (typep (second result) 'interrupted)))

(deftest putback-interrupted-while-disconnecting-releases-slot
  (let* ((gate (make-gate))
         (pool (make-pool :connector (make-counter)
                          :disconnector (lambda (conn)
                                          (declare (ignore conn))
                                          (pass-gate gate))
                          :max-open-count 1
                          :max-idle-count 0
                          :timeout 2000))
         (conn (fetch pool))
         (returning (spawn (lambda () (putback conn pool)))))
    (await-gate gate)
    (let ((waiter (start-waiting-fetch (lambda () (fetch pool)) (pool-lock pool))))
      (interrupt returning)
      (multiple-value-bind (finished result) (join-within returning 2)
        (unless finished
          (open-gate gate))
        (ok finished "The disconnect can still be interrupted")
        (ok (and finished (interrupted-p result))))
      (let ((result (bt2:join-thread waiter)))
        (ok (eq (first result) :ok) "The slot is released and the waiter woken")
        (ok (eql (second result) 2))))))

(deftest connect-does-not-block-other-borrowers
  (let* ((gate (make-gate))
         (blocking nil)
         (next-id (make-counter))
         (pool (make-pool :connector (lambda ()
                                       (when blocking
                                         (pass-gate gate))
                                       (funcall next-id))
                          :max-open-count 3)))
    (let ((a (fetch pool)))
      (fetch pool)
      (setf blocking t)
      (let ((connecting (spawn (lambda () (fetch pool)))))
        (await-gate gate)
        (ok (call-within 1 (lambda () (putback a pool)))
            "putback does not wait for the connect")
        (multiple-value-bind (finished conn) (call-within 1 (lambda () (fetch pool)))
          (ok finished "An idle connection is lent meanwhile")
          (ok (eql conn a)))
        (open-gate gate)
        (ok (equal (bt2:join-thread connecting) '(:ok 3)))
        (ok (= (pool-active-count pool) 3))))))

(deftest ping-does-not-block-other-borrowers
  (let* ((gate (make-gate))
         (blocked nil)
         (pool (make-pool :connector (make-counter)
                          :ping (lambda (conn)
                                  (when (eql conn blocked)
                                    (pass-gate gate))
                                  t)
                          :max-open-count 3)))
    (let ((a (fetch pool))
          (b (fetch pool)))
      (putback a pool)
      (setf blocked a)
      (let ((pinging (spawn (lambda () (fetch pool)))))
        (await-gate gate)
        (ok (call-within 1 (lambda () (putback b pool)))
            "putback does not wait for the ping")
        (multiple-value-bind (finished conn) (call-within 1 (lambda () (fetch pool)))
          (ok finished "Another idle connection is lent meanwhile")
          (ok (eql conn b)))
        (open-gate gate)
        (ok (equal (bt2:join-thread pinging) (list :ok a)))))))

(deftest waiting-for-a-connecting-slot-times-out
  (let* ((gate (make-gate))
         (pool (make-pool :connector (lambda () (pass-gate gate) :conn)
                          :max-open-count 1
                          :timeout 200)))
    (let ((connecting (spawn (lambda () (fetch pool)))))
      (await-gate gate)
      (multiple-value-bind (finished result)
          (call-within 2 (lambda ()
                           (handler-case (fetch pool)
                             (too-many-open-connection () :too-many))))
        (ok finished "The waiter is not stuck behind the connect")
        (ok (eq result :too-many)))
      (open-gate gate)
      (ok (equal (bt2:join-thread connecting) '(:ok :conn))))))

(deftest too-many-open-connection-is-signalled-without-the-pool-lock
  (dolist (timeout '(50 0))
    (let* ((pool (make-pool :connector (make-counter)
                            :max-open-count 1
                            :timeout timeout))
           (lock (pool-lock pool))
           (handled nil)
           (lock-free nil))
      (fetch pool)
      (multiple-value-bind (finished result)
          (call-within 5 (lambda ()
                           (handler-case
                               (handler-bind ((too-many-open-connection
                                                (lambda (c)
                                                  (declare (ignore c))
                                                  (setf handled t)
                                                  (when (ignore-errors (bt2:acquire-lock lock :wait nil))
                                                    (bt2:release-lock lock)
                                                    (setf lock-free t)))))
                                 (fetch pool))
                             (too-many-open-connection () :too-many))))
        (ok finished)
        (ok (eq result :too-many))
        (ok handled)
        (ok lock-free (format nil "The pool lock is free in the handler (timeout ~A)" timeout)))
      (ok (= (pool-active-count pool) 1)))))

(deftest connects-in-flight-count-toward-max-open
  (let* ((gate (make-gate))
         (next-id (make-counter))
         (pool (make-pool :connector (lambda ()
                                       (pass-gate gate)
                                       (funcall next-id))
                          :max-open-count 2
                          :timeout 300))
         (threads (loop repeat 4 collect (spawn (lambda () (fetch pool))))))
    (await-gate gate)
    (await-gate gate)
    (ng (bt2:wait-on-semaphore (gate-entered gate) :timeout 0.5) "No third connect starts")
    (open-gate gate 4)
    (let ((results (mapcar #'bt2:join-thread threads)))
      (ok (= (count :ok results :key #'first) 2))
      (ok (every (lambda (result)
                   (or (eq (first result) :ok)
                       (typep (second result) 'too-many-open-connection)))
                 results)))))

(deftest connector-error-releases-slot-and-wakes-waiter
  (let* ((gate (make-gate))
         (calls (make-counter))
         (pool (make-pool :connector (lambda ()
                                       (let ((n (funcall calls)))
                                         (when (= n 1)
                                           (pass-gate gate)
                                           (error "connect failed"))
                                         n))
                          :max-open-count 1
                          :timeout 2000)))
    (let ((failing (spawn (lambda () (fetch pool)))))
      (await-gate gate)
      (let ((waiter (start-waiting-fetch (lambda () (fetch pool)) (pool-lock pool))))
        (open-gate gate)
        (ok (eq (first (bt2:join-thread failing)) :error))
        (let ((result (bt2:join-thread waiter)))
          (ok (eq (first result) :ok) "The waiter is woken and opens a new connection")
          (ok (eql (second result) 2)))
        (ok (= (pool-active-count pool) 1))))))

(deftest retired-connection-slot-is-reused-without-waiting
  (let* ((disconnected '())
         (pool (make-pool :connector (make-counter)
                          :disconnector (lambda (conn) (push conn disconnected))
                          :ping (lambda (conn) (/= conn 1))
                          :max-open-count 1
                          :timeout 0)))
    (putback (fetch pool) pool)
    (ok (eql (fetch pool) 2) "The slot of the retired connection opens the replacement")
    (ok (equal disconnected '(1)))
    (ok (= (pool-active-count pool) 1))
    (ok (= (pool-idle-count pool) 0))))

(deftest replacement-connect-failure-releases-slot
  (let* ((calls (make-counter))
         (pool (make-pool :connector (lambda ()
                                       (let ((n (funcall calls)))
                                         (if (= n 2)
                                             (error "connect failed")
                                             n)))
                          :ping (constantly nil)
                          :max-open-count 1
                          :timeout 0)))
    (putback (fetch pool) pool)
    (ok (signals (fetch pool) 'simple-error))
    (ok (= (pool-active-count pool) 0))
    (ok (= (pool-open-count pool) 0))
    (ok (eql (fetch pool) 3) "The slot can be used again")))

(define-condition connect-progress (condition) ())

(deftest non-local-exit-from-connector-releases-slot
  (let ((pool (make-pool :connector (lambda ()
                                      (signal 'connect-progress)
                                      :conn)
                         :max-open-count 1
                         :timeout 0)))
    (ok (eq (block abort
              (handler-bind ((connect-progress (lambda (c)
                                                 (declare (ignore c))
                                                 (return-from abort :aborted))))
                (fetch pool)))
            :aborted))
    (ok (= (pool-active-count pool) 0))
    (ok (eq (fetch pool) :conn) "The slot is free again")))

(deftest non-local-exit-from-disconnector-in-cleanup-releases-slot
  (let* ((pool (make-pool :connector (make-counter)
                          :disconnector (lambda (conn)
                                          (declare (ignore conn))
                                          (throw 'disconnecting :thrown))
                          :ping (lambda (conn)
                                  (declare (ignore conn))
                                  (error "ping failed"))
                          :max-open-count 1
                          :timeout 0)))
    (putback (fetch pool) pool)
    (ok (eq (catch 'disconnecting
              (handler-case (fetch pool)
                (error () :ping-error)))
            :thrown)
        "The cleanup retires the connection being pinged")
    (ok (= (pool-active-count pool) 0) "The slot is released")
    (ok (eql (handler-case (fetch pool)
               (too-many-open-connection () :too-many))
             2)
        "The slot can be used again")))

(deftest interrupt-during-connect-releases-slot
  (let* ((gate (make-gate))
         (calls (make-counter))
         (pool (make-pool :connector (lambda ()
                                       (let ((n (funcall calls)))
                                         (when (= n 1)
                                           (pass-gate gate))
                                         n))
                          :max-open-count 1
                          :timeout 2000)))
    (let ((connecting (spawn (lambda () (fetch pool)))))
      (await-gate gate)
      (let ((waiter (start-waiting-fetch (lambda () (fetch pool)) (pool-lock pool))))
        (interrupt connecting)
        (multiple-value-bind (finished result) (join-within connecting 2)
          (unless finished
            (open-gate gate))
          (ok (and finished (interrupted-p result)) "The connect can be interrupted"))
        (let ((result (bt2:join-thread waiter)))
          (ok (eq (first result) :ok) "The waiter is woken")
          (ok (eql (second result) 2)))
        (ok (= (pool-active-count pool) 1))))))

(deftest interrupt-during-ping-retires-connection-and-releases-slot
  (dolist (args (list '() #+sbcl '(:idle-timeout 600000)))
    (let* ((gate (make-gate))
           (disconnected '())
           (pinging-enabled nil)
           (pool (apply #'make-pool
                        :connector (make-counter)
                        :disconnector (lambda (conn) (push conn disconnected))
                        :ping (lambda (conn)
                                (declare (ignore conn))
                                (when pinging-enabled
                                  (pass-gate gate))
                                t)
                        :max-open-count 1
                        :timeout 2000
                        args)))
      (putback (fetch pool) pool)
      (setf pinging-enabled t)
      (let ((pinging (spawn (lambda () (fetch pool)))))
        (await-gate gate)
        (let ((waiter (start-waiting-fetch (lambda () (fetch pool)) (pool-lock pool))))
          (interrupt pinging)
          (multiple-value-bind (finished result) (join-within pinging 2)
            (unless finished
              (open-gate gate))
            (ok (and finished (interrupted-p result)) "The ping can be interrupted"))
          (ok (equal disconnected '(1)) "The connection being pinged is disconnected")
          (let ((result (bt2:join-thread waiter)))
            (ok (eq (first result) :ok) "The waiter is woken")
            (ok (eql (second result) 2))))))))

(deftest with-connection-returns-connection-when-body-is-interrupted
  (let* ((gate (make-gate))
         (pool (make-pool :connector (make-counter)))
         (thread (spawn (lambda ()
                          (with-connection (conn pool)
                            (pass-gate gate)
                            conn)))))
    (await-gate gate)
    (interrupt thread)
    (multiple-value-bind (finished result) (join-within thread 2)
      (unless finished
        (open-gate gate))
      (ok (and finished (interrupted-p result))))
    (ok (= (pool-active-count pool) 0))
    (ok (= (pool-idle-count pool) 1) "The connection is put back")))

(deftest with-connection-lets-fetch-be-interrupted
  (let* ((gate (make-gate))
         (pool (make-pool :connector (lambda () (pass-gate gate) :conn)
                          :max-open-count 1))
         (thread (spawn (lambda ()
                          (with-connection (conn pool)
                            conn)))))
    (await-gate gate)
    (interrupt thread)
    (multiple-value-bind (finished result) (join-within thread 2)
      (unless finished
        (open-gate gate))
      (ok finished "The connect inside with-connection can be interrupted")
      (ok (and finished (interrupted-p result))))
    (ok (= (pool-active-count pool) 0))
    (ok (= (pool-idle-count pool) 0))))

(define-condition injected-failure (error) ())

;; Bound to T only where the worker catches INTERRUPTED; an interrupt landing anywhere else in the
;; thread (its start-up, or bt2's wrapper after the body returns) would go unhandled.
(defvar *armed* nil)

(deftest fetch-survives-failures-and-interrupts-under-concurrency
  (dolist (args (list '() #+sbcl '(:idle-timeout 1)))
    (let* ((stat-lock (bt2:make-lock :name "stats"))
           (live 0)
           (double-disconnects 0)
           (double-lends 0)
           (negative-counts 0)
           (unexpected-errors '())
           (lent (make-hash-table :test 'eq))
           (next-id (make-counter))
           (clock-lock (bt2:make-lock :name "clock"))
           (now 0)
           (interrupts 0)
           (stop nil)
           (pool (apply #'make-pool
                        :connector (lambda ()
                                     (when (zerop (random 20))
                                       (error 'injected-failure))
                                     ;; (id . disconnect-count)
                                     (let ((conn (cons (funcall next-id) 0)))
                                       (bt2:with-lock-held (stat-lock)
                                         (incf live))
                                       conn))
                        :disconnector (lambda (conn)
                                        (bt2:with-lock-held (stat-lock)
                                          (decf live)
                                          (when (> (incf (cdr conn)) 1)
                                            (incf double-disconnects))))
                        :ping (lambda (conn)
                                (declare (ignore conn))
                                (case (random 50)
                                  (0 (error 'injected-failure))
                                  ((1 2) nil)
                                  (t t)))
                        :max-open-count 3
                        :max-idle-count 3
                        :timeout 10000
                        ;; Every clock reading is one tick; connections live 20 ticks.
                        :max-lifetime (/ (* 20 1000) internal-time-units-per-second)
                        args)))
      (setf (pool-clock pool)
            (lambda () (bt2:with-lock-held (clock-lock) (incf now))))
      (let* ((workers
               (loop repeat 8
                     collect (bt2:make-thread
                              (lambda ()
                                (let ((*random-state* (make-random-state t)))
                                  ;; Interrupts land only inside an iteration; one pending when
                                  ;; the loop ends is caught here instead of killing the thread.
                                  (handler-case
                                      (let ((*armed* t))
                                        (without-interrupts*
                                          (loop repeat 1000
                                                do (handler-case
                                                       (with-local-interrupts*
                                                         (with-connection (conn pool)
                                                           ;; Bookkeeping is not interrupted halfway,
                                                           ;; or a stale entry would look like a double lend.
                                                           (without-interrupts*
                                                             (bt2:with-lock-held (stat-lock)
                                                               (if (gethash conn lent)
                                                                   (incf double-lends)
                                                                   (setf (gethash conn lent) t))
                                                               (when (or (minusp (pool-active-count pool))
                                                                         (minusp (pool-idle-count pool)))
                                                                 (incf negative-counts)))
                                                             (unwind-protect
                                                                  (with-local-interrupts*
                                                                    (when (zerop (random 4))
                                                                      (bt2:thread-yield)))
                                                               (bt2:with-lock-held (stat-lock)
                                                                 (remhash conn lent))))))
                                                     ((or interrupted too-many-open-connection injected-failure) ()
                                                       nil)
                                                     (error (e)
                                                       (bt2:with-lock-held (stat-lock)
                                                         (push e unexpected-errors)))))))
                                    (interrupted () nil)))))))
             (chaos
               (bt2:make-thread
                (lambda ()
                  (loop until stop
                        do (let ((target (nth (random (length workers)) workers)))
                             (when (ignore-errors
                                    (bt2:interrupt-thread target
                                                          (lambda ()
                                                            (when *armed*
                                                              (error 'interrupted))))
                                    t)
                               (incf interrupts)))
                           (sleep 0.001)))))
             (finished (every (lambda (worker) (join-within worker 120)) workers)))
        (setf stop t)
        (ok finished "Every worker finished")
        (ok (join-within chaos 10) "The chaos thread stopped")
        (when finished
          (when (getf args :idle-timeout)
            (sleep 0.2))
          (ok (plusp interrupts) "Interrupts were actually sent")
          (ok (null unexpected-errors) (format nil "No unexpected errors: ~S" unexpected-errors))
          (ok (= double-lends 0) "No connection lent to two borrowers at once")
          (ok (= double-disconnects 0) "No connection disconnected twice")
          (ok (= negative-counts 0))
          (ok (= (pool-active-count pool) 0) "Every slot came back")
          (ok (= (pool-open-count pool) (pool-idle-count pool)))
          (ok (= (hash-table-count (pool-created-at-table pool)) (pool-open-count pool)))
          ;; Interrupted right after the connector returns or inside the disconnector, a physical
          ;; connection can be left open; each such leak takes one interrupt.
          (ok (<= (pool-open-count pool) live (+ (pool-open-count pool) interrupts))
              (format nil "live=~A open=~A interrupts=~A" live (pool-open-count pool) interrupts)))))))
