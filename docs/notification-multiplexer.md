# PgNotificationMultiplexer

`PgNotificationMultiplexer` listens to many PostgreSQL `LISTEN` channels on **one** dedicated connection and dispatches
notifications to registered handlers asynchronously. Supports wildcards, per-channel concurrency limits with an
unbounded queue (nothing is dropped), and auto-reconnect with resubscribe.

---

## Key features

- **One connection, many channels** (exact and wildcard `prefix.*`)
- **Multiple handlers per channel**, added and removed at any time, also while `run()` is active
- **Nothing is dropped**: an unbounded FIFO per channel behind a limit of handler runs in flight
  (`channel_queue_capacity`)
- **Auto-reconnect** with **resubscribe**
- **No cost while idle**: the run loop sleeps on the socket, nothing polls

---

## Quick start

```cpp
#include "upq/PgNotificationMultiplexer.h"
using namespace usub::pg;

// 1) Implement a handler
struct MyHandler final : IPgNotifyHandler {
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel, std::string payload, int backend_pid) override
    {
        std::cout << "[NOTIFY] ch=" << channel
                  << " pid=" << backend_pid
                  << " payload=" << payload << "\n";
        co_return;
    }
};

usub::uvent::task::Awaitable<void> start_mux()
{
    // 2) Prepare a dedicated connection (do NOT put it back to the pool)
    auto conn = std::make_shared<PgConnectionLibpq>();
    auto err = co_await conn->connect_async(
        "host=127.0.0.1 port=5432 user=postgres dbname=app password=secret sslmode=disable"
    );
    if (err) co_return; // handle error yourself

    // 3) Create multiplexer (also pass creds for reconnects)
    PgNotificationMultiplexer mux{
        conn, "127.0.0.1", "5432", "postgres", "app", "secret",
        PgNotificationMultiplexer::Config{
            /*channel_queue_capacity*/             1024,   // handler runs in flight per channel
            /*pending_after_disconnect_capacity*/  2048,   // unused
            /*reconnect_backoff_us*/               100'000,
            /*max_recursive_depth*/                4,      // unused
            /*rate_limit_per_sec*/                 10'000  // unused
        }
    };

    // 4) Register handlers
    auto h_metrics = co_await mux.add_handler("metrics", std::make_shared<MyHandler>());
    auto h_alerts  = co_await mux.add_handler("alerts",  std::make_shared<MyHandler>());
    auto h_any_pay = co_await mux.add_handler("payments.*", std::make_shared<MyHandler>()); // wildcard

    if (!h_metrics || !h_alerts || !h_any_pay) co_return; // LISTEN failed

    // 5) Run forever (spawn in your loop)
    usub::uvent::system::co_spawn(mux.run());
    co_return;
}
```

---

## Wildcards

* Use `prefix.*` (must end with `.*`).
* Example: handler on `payments.*` will receive `payments.created`, `payments.updated`, etc.
* Exact and wildcard handlers can coexist.

---

## Subscriptions while running

Before `run()`, `add_handler()` sends its `LISTEN` itself; such calls must not overlap (await one before the next).
Once `run()` is active it is the only reader of the connection: a new subscription is queued, its `LISTEN` is sent
right away when the connection is free (otherwise after the statement in flight), and `add_handler()` returns when the
run loop has read the reply. `nullopt` means the `LISTEN` failed and the handler is not registered.

Removing the last handler of a channel queues its `UNLISTEN` the same way, without waiting for it: notifications that
arrive before it lands find no handler and are dropped. A later `add_handler()` for the same channel supersedes a
queued `UNLISTEN`.

All of this is safe from any worker: every libpq call on the connection happens under the multiplexer's mutex, in
short sections that never suspend.

## Backpressure

Nothing that reaches the multiplexer is dropped. Every notification is handed to each matching handler (resolved when
it arrives) in a detached coroutine of its own, started in arrival order. Per channel at most
`channel_queue_capacity` handler runs are in flight; further notifications wait in an unbounded FIFO and start as
runs finish — no polling, a finishing run starts the next one.

* `channel_queue_capacity = 1` — strictly one notification at a time, in order;
* `channel_queue_capacity = 0` — no limit, every notification starts at once.

`Stats::waiting` is the number of notifications queued right now. The `dropped_*` counters stay 0, and
`pending_after_disconnect_capacity`, `max_recursive_depth` and `rate_limit_per_sec` are unused; all are kept for
source compatibility. A queue that keeps growing means the handlers are slower than the notification rate.

Notifications sent while the listener is disconnected are never seen (PostgreSQL does not keep them for a session
that is gone): state that must not be missed needs a reconciliation pass after a reconnect.

```cpp
PgNotificationMultiplexer::Stats s = mux.stats();
std::cout << "waiting=" << s.waiting << "\n";
```

---

## Reconnect behavior

* On disconnect, the mux retries `connect_async` every `reconnect_backoff_us` with the host/port/user/db/password and
  the SSL / keepalive settings given to the constructor. Pass the real password: `PgPool::password()` only returns
  `"***"`, and a reconnect with it never succeeds.
* After reconnect it **resubscribes** to all exact channels and sends the subscription changes queued meanwhile.
* Wildcard registrations are kept in memory and continue to match.
* `run()` returns when its task is canceled.

---

## API

### Types

```cpp
struct IPgNotifyHandler {
    virtual usub::uvent::task::Awaitable<void>
    operator()(std::string channel, std::string payload, int backend_pid) = 0;
    virtual ~IPgNotifyHandler() = default;
};

struct PgNotificationMultiplexer::Config {
    size_t   channel_queue_capacity              = 256;      // handler runs in flight per channel, 0 = no limit
    size_t   pending_after_disconnect_capacity   = 1024;     // unused
    uint64_t reconnect_backoff_us                = 100'000;
    uint32_t max_recursive_depth                 = 4;        // unused
    uint32_t rate_limit_per_sec                  = 1000;     // unused
};

struct PgNotificationMultiplexer::HandlerHandle {
    uint64_t    id;
    std::string channel;   // exact or wildcard
    bool        wildcard;
};

struct PgNotificationMultiplexer::Stats {
    uint64_t dropped_overflow;      // always 0
    uint64_t dropped_recursive;     // always 0
    uint64_t dropped_rate_limited;  // always 0
    uint64_t waiting;               // notifications queued behind the per-channel limit
};
```

### Constructor

```cpp
PgNotificationMultiplexer(
    std::shared_ptr<PgConnectionLibpq> conn,
    std::string host, std::string port,
    std::string user, std::string db,
    std::string password,
    Config cfg = {}
);
```

> The initial `conn` is used immediately; credentials are stored for reconnects.

### Methods

| Method                                                                                                                                                                                                                       |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `task::Awaitable<std::optional<HandlerHandle>> add_handler(std::string channel, std::shared_ptr<IPgNotifyHandler> handler)` — Registers handler; `LISTEN` for exact channels; returns handle or `nullopt` on failure (nothing stays registered). |
| `bool remove_handler(const HandlerHandle& h)` — Unregisters a specific handler; for exact channels, queues the `UNLISTEN` when the last handler is removed.                                             |
| `bool remove_channel(const std::string& channel)` — Removes all handlers for the channel; exact channels are `UNLISTEN`ed (queued).                                                                   |
| `task::Awaitable<void> run()` — Main loop; consumes `PGnotify`, completes subscription changes, dispatches to handlers, handles reconnects.                                                          |
| `Stats stats() const` — Notifications waiting across all channels (the drop counters stay 0).                                                                                                                              |

---

## Handler execution model

* Handler runs start in arrival order; each one is `co_spawn`’d, so slow handlers don’t block the reader (they only
  hold back later notifications of their channel once `channel_queue_capacity` runs are in flight).
* An exception escaping a handler is caught and printed to stderr (`[UPQ/notify] handler for channel … threw`);
  handle failures inside the handler.

---

## When to dedicate the connection

Always. Don’t share the mux connection with general query load or return it to the pool. Keep it alive for the entire
process lifetime.