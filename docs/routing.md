# Routing

`PgConnector` provides intelligent routing between multiple PostgreSQL nodes (`Primary`, `Replica`, `Analytics`, etc.)
integrated tightly with `PgPool` and `PgTransaction`.
It automatically chooses the right connection pool for a query or transaction based on role, consistency policy,
replication lag, and health status.

---

## 🧩 Core Concepts

### Node Roles

| Role                      | Description           | Typical Use                             |
|---------------------------|-----------------------|-----------------------------------------|
| **Primary**               | Read/Write master     | All writes and strong reads             |
| **SyncReplica**           | Synchronous replica   | For `READ ONLY DEFERRABLE` transactions |
| **AsyncReplica**          | Asynchronous replica  | Eventual reads, high scalability        |
| **Analytics**             | OLAP / heavy queries  | Lower pool limit, non-critical          |
| **Archive / Maintenance** | Excluded from routing | Used for backups / offline tasks        |

---

### Consistency Policies

| Mode                 | Behavior                                        |
|----------------------|-------------------------------------------------|
| **Strong**           | Always route to Primary                         |
| **BoundedStaleness** | Route to replicas within defined lag thresholds |
| **Eventual**         | Route to any healthy replica                    |

---

### Node Health Metrics

Each node tracks:

* `healthy` (pinged via `SELECT 1`)
* `is_primary` (live leader/primary status, probed each health tick — see [Automatic Leader Detection](#automatic-leader-detection))
* Round-trip time (`rtt`)
* Replication lag (`replay_lag`, `lsn_lag`)
* Circuit breaker state (prevents retry storms)

---

## ⚙️ Configuration (C++)

```cpp
struct PgEndpoint {
    std::string name;
    std::string host, port, user, db, password;
    size_t max_pool{32};
    NodeRole role{NodeRole::AsyncReplica};
    uint8_t weight{1};
};

struct RoutingCfg {
    Consistency default_consistency{Consistency::Eventual};
    BoundedStalenessCfg bounded_staleness{std::chrono::milliseconds{150}, 0};
    uint32_t read_my_writes_ttl_ms{500};
};

struct HealthCfg {
    uint32_t interval_ms{500};
    uint32_t lag_threshold_ms{120};
    std::string rtt_probe_sql{"SELECT 1"};
    uint32_t cb_quiet_ms{500};
    uint32_t cb_backoff_ms{1000};
    uint32_t cb_max_ms{1500};
    bool auto_detect_leader{true};                          // follow the live leader across failovers
    std::string leader_probe_sql{"SELECT NOT pg_is_in_recovery()"};  // overridable leader check
};

struct Config {
    std::vector<PgEndpoint> nodes;
    std::vector<std::string> primary_failover;
    RoutingCfg routing{};
    PoolLimits limits{};
    TimeoutsMs timeouts{};
    HealthCfg health{};
};
```

---

## 🚀 Initialization

### A) Using `PgConnectorBuilder` (recommended)

```cpp
using namespace usub::pg;
using namespace std::chrono_literals;

PgConnector router = PgConnectorBuilder{}
  .node("p1","10.0.0.1","5432","app","maindb","***", NodeRole::Primary,1,64)
  .node("r1","10.0.0.2","5432","app","maindb","***", NodeRole::SyncReplica,2,64)
  .node("r2","10.0.0.3","5432","app","maindb","***", NodeRole::AsyncReplica,1,32)
  .node("olap","10.0.0.10","5432","app","maindb","***", NodeRole::Analytics,1,16)
  .primary_failover({"p1","r1","r2"})
  .default_consistency(Consistency::BoundedStaleness)
  .bounded_staleness(150ms,0)
  .read_my_writes_ttl(500ms)
  .pool_limits(64,16)
  .health(500,120,"SELECT 1")
  .auto_detect_leader(true)                       // optional: on by default
  .leader_probe_sql("SELECT NOT pg_is_in_recovery()")  // optional: override the leader check
  .build();
```

> `auto_detect_leader` defaults to `true` and `leader_probe_sql` defaults to
> `SELECT NOT pg_is_in_recovery()`, so neither call is required. Add them only to
> opt out of auto-detection or to plug in a custom topology check (e.g. Patroni).

### B) Using `Config` directly

```cpp
Config cfg;
cfg.nodes = {
  { "p1","10.0.0.1","5432","app","db","***", 64, NodeRole::Primary, 1 },
  { "r1","10.0.0.2","5432","app","db","***", 64, NodeRole::SyncReplica, 2 },
  { "r2","10.0.0.3","5432","app","db","***", 32, NodeRole::AsyncReplica, 1 },
};
cfg.primary_failover = {"p1","r1","r2"};
cfg.routing.default_consistency = Consistency::Eventual;
cfg.routing.bounded_staleness = {150ms, 0};
cfg.health = {500,120,"SELECT 1"};

PgConnector router{std::move(cfg)};
```

---

## 🩺 Health Monitoring

Run the health check coroutine in your event loop:

```cpp
usub::uvent::task::Awaitable<void> health_loop(PgConnector& r) {
    for (;;) {
        co_await r.health_tick();
        co_await usub::uvent::system::this_coroutine::sleep_for(500ms);
    }
}
```

This updates per-node:

* `healthy`
* `is_primary` (when `auto_detect_leader` is enabled — runs `leader_probe_sql` on each live node)
* `rtt`
* `replay_lag`, `lsn_lag`
* Circuit breaker state

Unhealthy nodes are excluded from routing until recovery.

> `start_health_loop()` is a convenience coroutine that runs `health_tick()` on the
> configured `health.interval_ms` cadence — `co_spawn` it once instead of writing your
> own loop.

---

## ⚙️ Routing Queries

### 1) Standard Read (Eventual Consistency)

```cpp
RouteHint rh{ .kind = QueryKind::Read, .consistency = Consistency::Eventual };
PgPool* pool = router.route(rh);
auto qr = co_await pool->query_awaitable("SELECT now()");
```

### 2) Read with Lag Constraint

```cpp
RouteHint rh{
  .kind = QueryKind::Read,
  .consistency = Consistency::BoundedStaleness,
  .staleness = {150ms, 0}
};
PgPool* pool = router.route(rh);
auto qr = co_await pool->query_awaitable("SELECT * FROM metrics LIMIT 100");
```

### 3) Writes or Strong Reads → Primary

```cpp
PgPool* poolW = router.route({ .kind=QueryKind::Write, .consistency=Consistency::Strong });
co_await poolW->query_awaitable("INSERT INTO audit(event) VALUES('login')");
```

### 4) Read-My-Writes Stickiness

```cpp
// Write
co_await router.route({QueryKind::Write, Consistency::Strong})
      ->query_awaitable("UPDATE users SET last_seen = now() WHERE id=$1", uid);

// Immediate read (forces Primary)
auto* poolR = router.route({
  .kind = QueryKind::Read,
  .consistency = Consistency::Eventual,
  .read_my_writes = true
});
auto qr = co_await poolR->query_awaitable("SELECT last_seen FROM users WHERE id=$1", uid);
```

### 5) Analytics Pin

```cpp
if (PgPool* olap = router.pin("olap", {})) {
    auto qr = co_await olap->query_awaitable("SELECT count(*) FROM large_table");
}
```

---

## 💾 Transaction Routing

### 6) Mapping `PgTransactionConfig → Pool`

```cpp
PgTransactionConfig cfg{
  .isolation  = TxIsolationLevel::Serializable,
  .read_only  = false,
  .deferrable = false
};

PgPool* ptx = router.route_for_tx(cfg);
PgTransaction tx(ptx, cfg);

if (co_await tx.begin()) {
    auto qr = co_await tx.query("INSERT INTO orders(id, amount) VALUES($1, $2)", 1, 500);
    (qr.ok) ? (void)co_await tx.commit() : (void)co_await tx.rollback();
}
```

### 7) Read-Only Deferrable TX (Prefers SyncReplica)

```cpp
PgTransactionConfig cfg{
  .isolation  = TxIsolationLevel::ReadCommitted,
  .read_only  = true,
  .deferrable = true
};
PgPool* ptx = router.route_for_tx(cfg);
PgTransaction tx(ptx, cfg);
co_await tx.begin();
auto count = co_await tx.select_one_reflect<int>("SELECT COUNT(*) FROM users");
co_await tx.commit();
```

### 8) Nested Savepoints (Subtransactions)

```cpp
PgTransactionConfig cfg{ .read_only = false };
PgTransaction tx(router.route_for_tx(cfg), cfg);
co_await tx.begin();

{
    auto sub = tx.make_subtx();
    co_await sub.begin();
    auto qr = co_await sub.query("UPDATE accounts SET amount = amount - $1 WHERE id=$2", 100, a1);
    if (!qr.ok) co_await sub.rollback();
    else        co_await sub.commit();
}

co_await tx.commit();
```

---

## 🧱 Service Layer Example

```cpp
class DbRouter {
public:
    explicit DbRouter(PgConnector r) : r_(std::move(r)) {}

    PgPool* read_eventual() { return r_.route({QueryKind::Read, Consistency::Eventual}); }
    PgPool* read_strong()   { return r_.route({QueryKind::Read, Consistency::Strong}); }
    PgPool* write()         { return r_.route({QueryKind::Write, Consistency::Strong}); }

    usub::uvent::task::Awaitable<void> start_health() {
        for (;;) { co_await r_.health_tick(); co_await usub::uvent::system::this_coroutine::sleep_for(500ms); }
    }

private:
    PgConnector r_;
};
```

---

## 🔄 Hot Reload / Failover

### 9) Hot Reload of Router Instance

```cpp
std::atomic<std::shared_ptr<PgConnector>> g_router;

void swap_router(PgConnector fresh) {
    g_router.store(std::make_shared<PgConnector>(std::move(fresh)));
}

PgPool* route_read() {
    auto r = g_router.load();
    return r ? r->route({QueryKind::Read, Consistency::Eventual}) : nullptr;
}
```

### 10) Automatic Failover

If `Primary` becomes unhealthy, the router automatically falls back to the next node listed in `primary_failover`.
When a replica is promoted by your cluster manager (Patroni, repmgr, managed RDS/Aurora, etc.), the router picks up
the new leader on the next health tick — see [Automatic Leader Detection](#automatic-leader-detection) below. You do
**not** need to rebuild the connector, swap it, or restart the process.

---

## 👑 Automatic Leader Detection

`auto_detect_leader` (enabled by default) lets the router follow the **current** primary at runtime instead of
trusting the static `NodeRole::Primary` you configured. This means a replica failover/promotion is handled without
restarting the pod or editing code.

### How it works

On every `health_tick()`, each reachable node is asked whether it is the leader by running `leader_probe_sql`
(default `SELECT NOT pg_is_in_recovery()`). The result updates `NodeStats::is_primary`, and write/strong routing
(`route`, `route_for_tx`) prefers the node that **reports** it is the leader — falling back to the configured role and
`primary_failover` order only when no leader has been detected yet.

The probe reads the **first column of the first row** as a boolean (`t`/`true`/`1` → leader), so it is independent of
the column name. Any SQL returning a single boolean works.

```cpp
PgConnector router = PgConnectorBuilder{}
    .node("pg-a","10.0.0.1","5432","app","db","***", NodeRole::Primary,1,64)
    .node("pg-b","10.0.0.2","5432","app","db","***", NodeRole::AsyncReplica,1,64)
    .node("pg-c","10.0.0.3","5432","app","db","***", NodeRole::AsyncReplica,1,64)
    .primary_failover({"pg-a","pg-b","pg-c"})
    .health(1000,200,"SELECT 1")
    .auto_detect_leader(true)
    .build();

usub::uvent::system::co_spawn(router.start_health_loop());

// After a failover the next write follows the new leader automatically — no restart.
RouteHint write{ .kind = QueryKind::Write, .consistency = Consistency::Strong };
if (auto* p = router.route(write))
    co_await p->query_awaitable("INSERT INTO audit(event) VALUES ('login')");
```

### Overriding the probe

Point `leader_probe_sql` at whatever defines leadership in your topology — as long as it returns one boolean row:

```cpp
// equivalent rephrasing of the default
.leader_probe_sql("SELECT pg_is_in_recovery() = false")

// example: only accept a primary that has a connected synchronous standby
.leader_probe_sql(
  "SELECT NOT pg_is_in_recovery() "
  "AND EXISTS (SELECT 1 FROM pg_stat_replication WHERE sync_state = 'sync')")
```

### Opting out

Set `auto_detect_leader(false)` to keep the legacy behavior: writes go strictly to the node configured as
`NodeRole::Primary` (and then down the `primary_failover` list). Use this when an external proxy already guarantees
the primary endpoint.

> A runnable end-to-end demo lives in `examples/main.cpp` (`auto_leader_routing_example`).

---

## ⚙️ Advanced Behavior

### Weighted Replica Selection

Between equally healthy replicas, the one with higher `weight` wins (after RTT comparison).

### Bounded Staleness

If replication lag exceeds threshold, replica is excluded:

```cpp
RouteHint rh{ QueryKind::Read, Consistency::BoundedStaleness, {80ms, 0} };
auto* pool = router.route(rh);
```

### Circuit Breaker Logic

When a node fails health check, it transitions to:

* **Open** → excluded for short timeout
* **Half-open** → retried once after interval
* **Closed** → back to routing on success

---

## 🧠 Full Example: Mixed Workload

```cpp
PgConnector router = PgConnectorBuilder{}
    .node("p1","10.0.0.1","5432","app","db","***", NodeRole::Primary,1,64)
    .node("r1","10.0.0.2","5432","app","db","***", NodeRole::SyncReplica,2,64)
    .build();

usub::uvent::task::Awaitable<void> workload() {
    // Write
    auto* pW = router.route({QueryKind::Write, Consistency::Strong});
    co_await pW->query_awaitable("UPDATE counters SET value = value + 1");

    // Read-after-write
    auto* pR = router.route({QueryKind::Read, Consistency::Eventual, {}, true});
    auto qr  = co_await pR->query_awaitable("SELECT value FROM counters");
}
```

---

## 📈 Observability

Collect per-node metrics during health tick:

* `healthy`
* `rtt_ms`
* `replay_lag_ms`
* `lsn_lag`
* `cb_state`

These can be exported to Prometheus or logs.

---

## 📘 Common Pitfalls

| Problem                              | Fix                                                |
|--------------------------------------|----------------------------------------------------|
| Mixed .h/.cpp definitions            | Keep declarations and definitions separate         |
| Missing health tick                  | The router never updates node states               |
| Reads after writes return stale data | Use `read_my_writes=true` or `Consistency::Strong` |
| Zero connection limits               | Adjust `PoolLimits` per node                       |
| Ignored replicas                     | Ensure `weight > 0` and node marked as usable      |