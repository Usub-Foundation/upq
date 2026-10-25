# PgPool

`PgPool` is an asynchronous PostgreSQL connection pool.  
It manages live `PGconn` sockets, hands them out to coroutines, and safely recycles (or retires) them.

Primary high-level entrypoint for queries in **upq**.

---

## Overview

`PgPool` provides:

- Bounded pooling via `max_pool_size`
- Non-blocking acquisition (`co_await` suspends coroutines, not threads)
- Safe recycling with dirty-connection handling
- Structured errors (`QueryResult`)
- Optional periodic health checks (`PgPoolHealthConfig`)
- **Reflective parameter/result mapping**
    - **Preferred**: `query_reflect_expected*` (error-aware)
    - **Deprecated**: `query_reflect*` (exception/optional-only)
- Full compatibility with low-level features on `PgConnectionLibpq`:
    - `COPY ... FROM STDIN` / `COPY ... TO STDOUT`
    - Server-side cursors with chunked fetch

---

## Construction

```cpp
usub::pg::PgPool pool{
    /*host*/     "127.0.0.1",
    /*port*/     "5432",
    /*user*/     "postgres",
    /*db*/       "app",
    /*password*/ "secret",
    /*max_pool*/ 32
};
```

---

## Connection lifecycle

```cpp
auto conn = co_await pool.acquire_connection();
// use conn...
pool.release_connection(conn);                 // fast path
co_await pool.release_connection_async(conn);  // drain & recycle
```

### Dirty-connection handling

If a connection returns with unread results/pending input:

* `release_connection(conn)` **retires** the connection.
* `release_connection_async(conn)` drains pending results and **recycles** (an idle connection is recycled at
  once, without touching the socket).

Pool statements never leave unread results behind, and after a server `ERROR` (constraint, no rows, …) the
connection goes back to the pool: only connection-level failures retire it.

### Marking a connection dead

```cpp
pool.mark_dead(conn); // retires it; do not reuse
```

### Connecting and the socket

`connect_async(conninfo, budget)` (default budget 5 s) waits on socket readiness for every step of the handshake,
so a new session costs what libpq's handshake costs (TCP, TLS, SCRAM), not a polling interval per step. A detached
timer bounds the whole handshake: a server that accepts TCP and never answers fails with
`"connect timeout after N ms"`.

libpq owns its socket; the event loop watches a `dup` of it. Each side closes only its own descriptor, so a
connection closed on another worker than the one that opened it (the usual case in a pool), or one the poller saw
hang up, never closes a descriptor number that the process has meanwhile given to another socket.

---

## High-level query API

One-shot:

```cpp
auto res = co_await pool.query_awaitable(
    "UPDATE users SET name=$1 WHERE id=$2 RETURNING name;",
    "John", 1
);

if (res.ok) {
    // res.rows[0].cols[0] == "John"
}
```

Pinned connection:

```cpp
auto conn = co_await pool.acquire_connection();
auto res  = co_await pool.query_on(conn, "SELECT now()");
pool.release_connection(conn);
```

Both return `QueryResult{ ok, code, error, err_detail, rows, rows_valid }`.

---

## Reflect-aware API (error-aware, **preferred**)

Returns `std::expected<..., PgOpError>`;

```cpp
struct PgOpError {
    PgErrorCode code;
    std::string error;
    PgErrorDetail err_detail; // { sqlstate, message, detail, hint, category }
};
```

### SELECT → expected<vector<T>> / expected<T>

```cpp
struct UserRow {
    int64_t id;
    std::string username;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

// list
auto users = co_await pool.query_reflect_expected<UserRow>(
    "SELECT id, name AS username, password, roles, tags FROM users ORDER BY id;"
);

if (!users) {
    const auto& e = users.error();
    std::cout << "fail: " << e.error << " (" << usub::pg::toString(e.code) << ")\n";
}

// one
auto one = co_await pool.query_reflect_expected_one<UserRow>(
    "SELECT id, name AS username, password, roles, tags FROM users WHERE id = $1",
    1
);

if (!one) {
    const auto& e = one.error();
    // e.error == "no rows" if SELECT matched nothing
}
```

### INSERT/UPDATE from aggregates or tuples (через `exec_reflect` + проверка `QueryResult`)

Для DML удобней оставить `QueryResult`:

```cpp
struct NewUser {
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

NewUser u{"bob", std::nullopt, {1,2}, {"vip"}};
auto r = co_await pool.exec_reflect(
    "INSERT INTO users(name,password,roles,tags) VALUES($1,$2,$3,$4)", u);
if (!r.ok) { /* r.error, r.err_detail.sqlstate */ }
```

### Mapping rules (unchanged)

* Field names map to column names (aliases via `AS` supported)
* `std::optional<T>` ↔ `NULL`
* STL containers ↔ PG arrays
* Aggregates/tuples expand into `$1..$N`

### API summary (preferred)

| Method                                          | Description                                     |
|-------------------------------------------------|-------------------------------------------------|
| `query_reflect_expected<T>(sql)`                | `expected<vector<T>, PgOpError>` (no params)    |
| `query_reflect_expected<T>(sql, args...)`       | `expected<vector<T>, PgOpError>` (with params)  |
| `query_reflect_expected_one<T>(sql)`            | `expected<T, PgOpError>` (no params)            |
| `query_reflect_expected_one<T>(sql, args...)`   | `expected<T, PgOpError>` (with params)          |
| `query_on_reflect_expected<T>(conn, sql, ... )` | Connection-pinned variants                      |
| `exec_reflect(sql, obj)`                        | DML with aggregate/tuple params → `QueryResult` |

### Legacy reflect API (**Deprecated**)

| Method                           | Return             | Status     |
|----------------------------------|--------------------|------------|
| `query_reflect<T>(sql, ...)`     | `std::vector<T>`   | Deprecated |
| `query_reflect_one<T>(sql, ...)` | `std::optional<T>` | Deprecated |
| `query_on_reflect*`              | pinned variants    | Deprecated |

Используй `*_expected` вместо них.

---

## Bulk COPY (via `PgConnectionLibpq`)

```cpp
auto conn = co_await pool.acquire_connection();

auto st = co_await conn->copy_in_start("COPY public.items(v) FROM STDIN");
if (!st.ok) { pool.release_connection(conn); co_return; }

for (int i = 0; i < 5; i++) {
    std::string line = std::to_string(i) + "\n";
    auto r = co_await conn->copy_in_send_chunk(line.data(), line.size());
    if (!r.ok) { pool.release_connection(conn); co_return; }
}

auto fin = co_await conn->copy_in_finish(); // PgCopyResult
pool.release_connection(conn);
```

---

## Server-side cursors (chunked fetch)

```cpp
auto conn = co_await pool.acquire_connection();
auto name = conn->make_cursor_name();

auto decl = co_await conn->cursor_declare(name,
    "SELECT id, payload FROM public.bigdata ORDER BY id");
if (!decl.ok) { pool.release_connection(conn); co_return; }

for (;;) {
    auto ch = co_await conn->cursor_fetch_chunk(name, 3);
    if (!ch.ok || ch.rows.empty()) break;
    // use rows...
    if (ch.done) break;
}

auto cls = co_await conn->cursor_close(name);
pool.release_connection(conn);
```

---

## Error model

* No exceptions from pool API — structured results only.
* `QueryResult` contains: `ok`, `code`, `error`, `err_detail{sqlstate, detail, hint, category}`
* `*_expected` returns `std::expected<..., PgOpError>`

### Retries inside the pool

Every pool-level statement (`query_awaitable`, `query_reflect_expected[_one]`, `exec_reflect`, the deprecated
`query_reflect*`) acquires a connection, runs, gives the connection back and then decides about a retry from
one classification of the failure, `usub::pg::classify_retry()` in `upq/PgRetry.h`:

| `RetryClass` | What happened | Retried |
|---|---|---|
| `NotStarted` | pooler/server refused before running anything: `08P01`, `28P01`, `53300`, `57P03` | always: pool's attempts (`set_transient_retry`, default 3) with backoff `base * 3^i` |
| `ConnectionLost` | socket died under the statement (EOF/RST, other `08xxx`, `57P01/02`, `25P03`, libpq "server closed the connection" …) | once, immediately, with `.idempotent` (or the older `.retry_on_connection_loss`) |
| `Deadline` | `io_deadline_ms` cut the await; the statement may still run | once, immediately, with `.idempotent` |
| `RolledBack` | `40001`, `40P01`, `55P03`: the server rolled the statement back | up to attempts−1 times, backoff 50/100/200 ms, with `.idempotent` |
| `None` | the statement's own error (constraint, syntax, no rows), cancellation | never |

`QueryOpts::idempotent` is the caller's promise that running the statement twice is harmless (a `SELECT`, an
upsert keyed by a client-generated id). `QueryOpts::retry_budget_ms` bounds the whole call: a retry that would
start after it is not made.

```cpp
usub::pg::QueryOpts opts{.io_deadline_ms = 3000, .idempotent = true, .retry_budget_ms = 8000};
auto rows = co_await pool.query_reflect_expected<Row>(opts, "SELECT ... WHERE id = $1", id);
```

Every retry is counted in `HealthStats` (`transient_retries`, `connection_loss_retries`, `deadline_retries`,
`rollback_retries`) and reported to the retry observer before its backoff:

```cpp
pool.set_retry_observer([](const usub::pg::RetryEvent &ev) {
    ulog::warn("pg retry | class= ({}) | attempt= ({}) | sqlstate= ({}) | db= ({}) | msg= ({})",
               usub::pg::toString(ev.cls), ev.attempt, ev.sqlstate, ev.db, ev.message);
});
```

The observer runs on the worker that ran the statement and must not suspend; install it before the pool is
used. Without one, each retry prints one line to stderr (the NotStarted line keeps its historical wording).

A whole transaction is retried with `usub::pg::run_tx()`, see [transaction.md](transaction.md).

### Stale idle connections

Before an idle connection is handed out, the pool peeks at its socket (`recv(MSG_PEEK | MSG_DONTWAIT)`): a
connection the server or pooler closed while it sat idle (restart, idle timeout, `pg_terminate_backend`) is
dropped there, before anything is sent, and counted in `HealthStats::stale_dropped`. This catches orderly
closes and resets the poller already saw; a peer that vanished silently is still found by the IO deadline. 