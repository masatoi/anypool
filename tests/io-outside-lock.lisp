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
