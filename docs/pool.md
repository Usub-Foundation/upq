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
* `release_connection_async(conn)` drains pending results and **recycles**.

### Marking a connection dead

```cpp
pool.mark_dead(conn); // retires it; do not reuse
```

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

The pool-level entry points (`query_awaitable`, `query_reflect_expected`, `exec_reflect`) run a statement on a
freshly acquired connection and apply two independent retry policies:

* **Transient pooler SQLSTATEs** (`08P01`, `28P01`, `53300`, `57P03`) — the pooler/server rejected the
  session before running anything; the pool retries with exponential backoff (`transient_retries` in
  `HealthStats`). Always on, safe for writes.
* **Connection loss** — `ConnectionClosed` / `SocketReadFailed` without an IO-deadline abort: the pooled
  socket died under the statement (peer EOF/RST, pooler restart). Opt-in per query via
  `QueryOpts{.retry_on_connection_loss = true}`: one immediate re-run on a fresh connection
  (`connection_loss_retries` in `HealthStats`). **The caller asserts idempotency** — a mid-flight loss
  cannot tell whether the server applied the statement, so this is for SELECTs and upserts keyed by a
  client-generated id. IO-deadline aborts (`io_timed_out()`) and server `ERROR`s are never retried here.

```cpp
usub::pg::QueryOpts opts{.io_deadline_ms = 3000, .retry_on_connection_loss = true};
auto rows = co_await pool.query_reflect_expected<Row>(opts, "SELECT ... WHERE id = $1", id);
``` 