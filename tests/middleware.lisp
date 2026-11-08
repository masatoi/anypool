(defpackage #:anypool/tests/middleware
  (:use #:cl #:rove)
  (:import-from #:anypool/middleware
                #:*connection*
                #:*lack-middleware-anypool*))

(in-package #:anypool/tests/middleware)

(deftest max-lifetime-is-forwarded-to-the-pool
  (let* ((calls 0)
         (closed '())
         (app (funcall *lack-middleware-anypool*
                       (lambda (env) (declare (ignore env)) *connection*)
                       :connector (lambda () (list (incf calls)))
                       :disconnector (lambda (conn) (push conn closed))
                       :max-lifetime 100)))
    (let ((first (funcall app nil)))
      (sleep 0.2)
      (let ((second (funcall app nil)))
        (ng (eq first second) "The expired connection is replaced")
        (ok (= calls 2))
        (ok (equal closed (list first)))))))

(define-condition middleware-application-failure (error) ())

(deftest app-error-is-not-masked-by-an-expired-connection-close-error
  (let ((app (funcall *lack-middleware-anypool*
                      (lambda (env)
                        (declare (ignore env))
                        (sleep 0.2)
                        (error 'middleware-application-failure))
                      :connector (lambda () (list :connection))
                      :disconnector (lambda (conn)
                                      (declare (ignore conn))
                                      (error "close failed"))
                      :max-lifetime 100)))
    (ok (signals (funcall app nil) 'middleware-application-failure))))

(deftest middleware-can-use-a-caller-owned-pool-for-recovery
  (let* ((now 0)
         (pool (anypool:make-pool :connector (lambda () (list :connection))
                                  :disconnector (lambda (conn)
                                                  (declare (ignore conn))
                                                  (error "close failed"))
                                  :max-open-count 1
                                  :max-lifetime 1000
                                  :timeout 0)))
    (setf (anypool::pool-clock pool) (lambda () now))
    (let ((app (funcall *lack-middleware-anypool*
                        (lambda (env)
                          (declare (ignore env))
                          (setf now (* 2 internal-time-units-per-second)))
                        :pool pool)))
      (ok (signals (funcall app nil) 'simple-error))
      (let ((record (first (anypool:pool-failed-disconnections pool))))
        (ok record "The caller can inspect the middleware's failed connection")
        (when record (ok (anypool:acknowledge-failed-disconnection pool record))))
      (ok (= (anypool:pool-open-count pool) 0))
      (ok (= (funcall app nil) (* 2 internal-time-units-per-second))
          "The middleware can handle requests again after acknowledgment"))))

(deftest middleware-rejects-pool-options-with-a-caller-owned-pool
  (let ((pool (anypool:make-pool :connector (lambda () (list :connection)))))
    (ok (functionp (funcall *lack-middleware-anypool* #'identity :pool pool)))
    (ok (signals (funcall *lack-middleware-anypool* #'identity
                          :pool pool :max-lifetime 1000)
                 'error))
    (ok (signals (funcall *lack-middleware-anypool* #'identity :pool nil) 'type-error))
    (ok (signals (funcall *lack-middleware-anypool* #'identity :pool :not-a-pool) 'type-error))))

(defvar *custom-connection* :outer)

(deftest caller-owned-pool-honors-the-variable-option
  (let* ((conn (list :connection))
         (pool (anypool:make-pool :connector (constantly conn)))
         (app (funcall *lack-middleware-anypool*
                       (lambda (env) (declare (ignore env)) *custom-connection*)
                       :pool pool :var '*custom-connection*)))
    (ok (eq (funcall app nil) conn))
    (ok (eq *custom-connection* :outer))))

(deftest lack-builder-uses-a-caller-owned-pool-for-recovery
  (let* ((closed nil)
         (fail-close t)
         (pool (anypool:make-pool :connector (lambda () (list :connection))
                                  :disconnector (lambda (conn)
                                                  (declare (ignore conn))
                                                  (setf closed t)
                                                  (when fail-close
                                                    (error "close failed")))
                                  :max-open-count 1
                                  :max-idle-count 0
                                  :timeout 0))
         (app (lack:builder (:anypool :pool pool)
                (lambda (env) (declare (ignore env)) :ok))))
    (ok (signals (funcall app nil) 'simple-error))
    (ok closed)
    (ok (signals (funcall app nil) 'anypool:too-many-open-connection))
    (let ((record (first (anypool:pool-failed-disconnections pool))))
      (ok record)
      (when record (ok (anypool:acknowledge-failed-disconnection pool record))))
    (setf fail-close nil)
    (ok (eq (funcall app nil) :ok))
    (ok (= (anypool:pool-open-count pool) 0))))
