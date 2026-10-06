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
    (open-gate gate 2)
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
