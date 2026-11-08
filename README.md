# anypool

[![Build Status](https://github.com/fukamachi/anypool/workflows/CI/badge.svg)](https://github.com/fukamachi/anypool/actions?query=workflow%3ACI)

General-purpose connection pooling library.

## WARNING

This software is still ALPHA quality. The APIs will be likely to change.

## Usage

```common-lisp
(ql:quickload '(:anypool :dbi))
(use-package :anypool)

(defvar *connection-pool*
  (make-pool :name "dbi-connections"
             :connector (lambda ()
                          (dbi:connect :postgres
                                       :database-name "webapp"
                                       :username "fukamachi" :password "1ove1isp"))
             :disconnector #'dbi:disconnect
             :ping #'dbi:ping
             :max-open-count 10
             :max-idle-count 2))

(fetch *connection-pool*)
;=> #<DBD.POSTGRES:DBD-POSTGRES-CONNECTION {10054E07C3}>

(pool-open-count *connection-pool*)
;=> 1

(pool-idle-count *connection-pool*)
;=> 0

(putback *** *connection-pool*)
; No value

(pool-open-count *connection-pool*)
;=> 1

(pool-idle-count *connection-pool*)
;=> 1

;; Using `with-connection` macro.
(with-connection (mito:*connection* *connection-pool*)
  (mito:find-dao 'user :name "fukamachi"))

;; Unlimited connections mode (never blocks)
(defvar *object-pool*
  (make-pool :name "object-pool"
             :connector #'make-object
             :max-open-count nil      ; Unlimited!
             :max-idle-count 100))    ; But only keep 100 idle

;; This will never block, even under high load
(fetch *object-pool*)  ;=> Always succeeds

;; Stop reusing connections 5 minutes after they were opened
;; (5 minutes is only an example, not a recommended value).
(defvar *reader-pool*
  (make-pool :name "reader-connections"
             :connector (lambda ()
                          (dbi:connect :postgres
                                       :host "reader.example.com"
                                       :database-name "webapp"
                                       :username "fukamachi" :password "1ove1isp"))
             :disconnector #'dbi:disconnect
             :ping #'dbi:ping
             :max-lifetime (* 5 60 1000)))
```

## API

### [Constructor] make-pool (&key name connector disconnector ping max-open-count max-idle-count timeout idle-timeout max-lifetime)

- `:name`: The name of a new pool. This is used to print the object.
- `:connector` (Required): A function to make and return a new connection object. It takes no arguments and may return `nil` as a resource. It runs without the pool lock and may be called from several threads at once, so it must be thread-safe. Equal `eql` identities returned by overlapping calls must refer to the same physical resource; do not reuse an identity for a different resource while calls overlap.
- `:disconnector`: A function to disconnect a given object. It takes a single argument returned by `:connector`. It runs without the pool lock and may be called concurrently. See [failed disconnections](#failed-disconnections) for error handling.
- `:ping`: A function to check if the given object is still available. It takes a single argument. It runs without the pool lock and may be called from several threads at once. If it returns `nil`, the object is disconnected and `fetch` tries another one. If it signals an error, the pool attempts to disconnect the object and propagates the original error. A failed disconnect is recorded even when the original ping error takes precedence.
- `:max-open-count`: The maximum number of concurrently open connections. The default is `4` and can be configured with `*default-max-open-count*`. If `nil`, allows unlimited connections (never blocks or errors).
- `:max-idle-count`: The maximum number of idle/pooled connections. The default is `2` and can be configured with `*default-max-idle-count*`.
- `:timeout`: The milliseconds to wait in `fetch` when the number of open connection reached to the maximum. A slot in which another `fetch` is still connecting or pinging counts as open, so this also bounds how long `fetch` waits behind a slow connect. It bounds each wait, not the whole call: if another thread takes the slot that woke `fetch`, it waits again. If `nil`, it waits forever. The default is `nil`.
- `:idle-timeout`: The milliseconds to disconnect idle resources after they're `putback`ed to the pool. If `nil`, it won't disconnect automatically. The default is `nil`.
- `:max-lifetime`: The milliseconds after which a connection is no longer reused, counted from when `:connector` returned it. It must be a positive real number; `0`, negative numbers and non-numbers are rejected. If `nil`, connections are reused regardless of their age. The default is `nil`. See [max-lifetime](#max-lifetime).

The three time settings are independent:

- `:timeout` is how long `fetch` waits for a free slot when `max-open-count` is reached.
- `:idle-timeout` discards a connection that has stayed idle in the pool for that long. Each `putback` starts a new idle period.
- `:max-lifetime` stops reusing a connection once that long has passed since it was created, however often it has been used.

### [Accessor] pool-max-open-count (pool)

Return the maximum number of concurrently open connections. If the number of open connections reached to the limit, `fetch` waits until a connection is available.

### [Accessor] (setf pool-max-open-count) (new-max-open-count pool)

Set the maximum number of concurrently open connections.

### [Accessor] pool-max-idle-count (pool)

Return the maximum number of idle/pooled connections. If the number of idle connections reached to the limit, extra connections will be disconnected in `putback`.

### [Accessor] (setf pool-max-idle-count) (new-max-idle-count pool)

Set the maximum number of idle/pooled connections.

### [Accessor] pool-idle-timeout (pool)

Return the milliseconds to disconnect idle resources after they're `putback`ed to the pool. If `nil`, this feature is disabled.

### [Accessor] (setf pool-idle-timeout) (new-idle-timeout pool)

Set the milliseconds to disconnect idle resources after they're `putback`ed to the pool. If `nil`, this feature is disabled.

### [Accessor] pool-max-lifetime (pool)

Return the `:max-lifetime` given to `make-pool`, or `nil` if it is disabled. It is fixed when the pool is created and cannot be changed afterwards.

### [Accessor] pool-open-count (pool)

Return the number of occupied or reserved slots. The count is the sum of `pool-active-count` and `pool-idle-count`; it includes connections being opened or closed and failed disconnections awaiting acknowledgment.

### [Accessor] pool-active-count (pool)

Return the number of slots in use or reserved for a connection being opened, checked, closed, or awaiting acknowledgment of a failed disconnect.

### [Accessor] pool-idle-count (pool)

Return the number of currently idle connections.

### [Accessor] pool-overflow-count (pool)

Return the number of currently active connections beyond `pool-max-open-count`. Always returns `0` for pools created with `:max-open-count nil` (unlimited) or for limited pools (which never exceed their limit).

### [Function] fetch (pool)

Return an available resource from the `pool`. If no open resources available, it makes a new one with a function specified to `make-pool` as `:connector`.

No open resource available and can't open due to the `max-open-count`, this function waits for `:timeout` milliseconds. If `:timeout` specified to `nil`, this waits forever until a new one turns to available.

### [Function] putback (object pool)

Send an in-use connection back to `pool` to make it reusable by other threads. If the number of idle connections is reached to `pool-max-idle-count`, the connection will be disconnected immediately.

### Lack middleware

The `anypool/middleware` system accepts the same pool constructor options, including `:max-lifetime`. Alternatively, pass an existing pool as `:pool`, without other pool constructor options. Keeping the pool reference lets the caller inspect and acknowledge failed disconnections even when Lack constructs the middleware stack. `:var` can be used with either form.

```lisp
(funcall anypool/middleware:*lack-middleware-anypool* app :pool *connection-pool*)
```

### [Function] pool-failed-disconnections (pool)

Return a snapshot of records for failed disconnections and connections whose identity could not be verified. Each record has read-only accessors `failed-disconnection-connection` and `failed-disconnection-condition`; the condition is `nil` if the disconnect exited without signalling an error. These records continue to occupy slots.

### [Function] acknowledge-failed-disconnection (pool record)

After verifying that the record's physical connection is closed and not in use by another borrower, release its reserved slot. Return true if the record belonged to the pool and was still awaiting acknowledgment; return `nil` for a stale record or a record from another pool. This function does not call the disconnector.

## Failed disconnections

If a disconnector signals an error or exits before it completes, the pool cannot know whether the resource was actually closed. It retains the resource and its slot in a failed-disconnection record rather than automatically retrying or opening a replacement. A direct disconnect error propagates to the caller. If disconnecting during cleanup after another error also fails, the original error propagates and the disconnect failure is recorded. An idle-timer disconnect has no caller, so inspect `pool-failed-disconnections` to find failures there.

An operator can inspect each record, close the resource out of band if necessary, and call `acknowledge-failed-disconnection` only after verifying it is closed. A failed slot remains reserved until acknowledgment; waiters can time out or continue to wait. Retrying blindly can close an already-closed resource twice. `with-connection` preserves an error or other non-local exit from its body if cleanup signals an error; the failed disconnect remains recorded.

When `:max-lifetime` is disabled and a connector returns the same object for several slots, the pool disconnects it only after its last slot is retired. If a ping rejects a shared object, its remaining idle aliases are retired rather than lent; aliases already borrowed are retired on return.

A connector can overlap the retirement of another connection with the same identity. In that case the pool rejects the returned object rather than lend or disconnect a resource that may already be closed. To bound memory, each in-progress connector tracks at most 4,096 distinct retired identities. If it overlaps more retirements and returns an identity that cannot be classified, `fetch` signals `connection-identity-uncertain` and retains the object and its slot in a failed-disconnection record. The pool has not attempted a disconnect in this case. The resource may be new, already closed, or shared, so an operator must verify its identity and closure before acknowledgment. A long-running connector during heavy connection churn can therefore reduce available pool capacity until this recovery is performed.

## Interrupts

On SBCL, `fetch`, `putback` and `with-connection` keep their slot accounting correct when the thread is interrupted, for example by `sb-ext:with-timeout` or `interrupt-thread`. The connector, the ping, the disconnector and the body of `with-connection` can still be interrupted.

- Use `with-connection` where you can. It defers interrupts from the moment `fetch` returns until the connection is bound, and while it is put back. If you call `fetch` and `putback` directly, an interrupt between `fetch` returning and your code taking the connection loses the connection and its slot.
- If `conn` in `with-connection` names a special variable, it is bound to `nil` while `fetch` runs.
- If the connector is interrupted before returning an object, it must clean up any resources it has already opened. Once the pool has received the object, it attempts to disconnect it if `fetch` exits before lending it.
- An interrupt during disconnection leaves a failed-disconnection record and retains the slot. Non-local exits and conditions such as `sb-ext:timeout` still take effect.
- On other implementations there is no protection against interrupts.

## max-lifetime

`:max-lifetime` lets a pool replace healthy connections on a schedule. It is meant for endpoints that pick a server when a connection is opened, such as a database reader endpoint: while old connections keep working, they never go back through that choice, so servers added later get no share of them. Once a connection reaches its lifetime, the pool stops lending it, and the next `fetch` that needs a connection calls `:connector` again.

What it guarantees:

- `fetch` checks a connection's age before lending it. An idle connection is checked again after `:ping` returns. If a new connection expires while waiting to be registered, it is retired and `fetch` signals `connection-expired-before-use`; it does not repeatedly connect forever when the configured lifetime is too short.
- Using a connection does not extend its lifetime. The creation time is kept across `fetch` and `putback`.
- A connection that expires while borrowed is not touched. It is disconnected when it is `putback`, and a thread waiting in `fetch` is woken to open a new one.
- It works with `:ping nil` and with a `:ping` that always succeeds. An expired connection is dropped without calling `:ping`.
- If `:disconnector` fails for an expired connection, the connection is not reused and the failure remains recorded until it is acknowledged.

What it does not do:

- It never disconnects a borrowed connection. A connection kept borrowed for a long time is used past its lifetime until it is `putback`.
- There is no timer for it. Expired connections are found in `fetch` and `putback`, so a pool that is not being used keeps them until it is used again (`:idle-timeout` still disconnects idle ones on its own schedule).
- It does not touch DNS or any cache. `:connector` has to open a new physical connection each time; if it hands out connections from another cache of its own, the old ones may come back.
- `:connector` must return a new object on each call while `:max-lifetime` is set. `fetch` signals an error if it returns an object the pool already manages, and leaves that object alone.
- Without `:disconnector`, an expired connection is only dropped from the pool; nothing releases its external resources explicitly.
- It gives new servers a chance to receive connections, but it does not balance load evenly among them.
- It does not make database changes safe by itself. If a server is stopped first, work on connections to it can fail.
- Connections opened at about the same time expire at about the same time, and are replaced together.

## Author

* Eitaro Fukamachi (e.arrows@gmail.com)

## Copyright

Copyright (c) 2020 Eitaro Fukamachi (e.arrows@gmail.com)

## License

Licensed under the BSD 2-Clause License.
