# PgTransaction

`PgTransaction` provides coroutine-safe transactional operations for PostgreSQL using a dedicated pooled connection.

Wraps `BEGIN` / `COMMIT` / `ROLLBACK`, manages connection reuse, supports subtransactions (`SAVEPOINT`), and integrates
reflection-based mapping.

---

## Overview

- Dedicated connection from `PgPool`
- Configurable isolation / read-only / deferrable
- Async queries on the same pinned connection
- Subtransactions via `SAVEPOINT`
- Reflect-aware helpers
    - **Preferred**: `query_reflect_expected*` / aliases `select*_expected`
    - **Deprecated**: `query_reflect*` / `select_reflect*`
- Optional pipeline mode (compile-time flag)

All methods are coroutine-awaitable.

---

## Configuration

```cpp
usub::pg::PgTransactionConfig cfg{
    .isolation  = usub::pg::TxIsolationLevel::Serializable,
    .read_only  = false,
    .deferrable = false
};

usub::pg::PgTransaction txn(&pool, cfg);
co_await txn.begin();
```

Generated SQL example:

```
BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE;
```

---

## Executing queries

```cpp
auto r = co_await txn.query(
    "INSERT INTO logs(message) VALUES($1) RETURNING id;",
    "started"
);
if (!r.ok) {
    // r.error, r.err_detail.sqlstate
    co_await txn.rollback();
}
```

---

## Reflect-based queries (error-aware, **preferred**)

Return via `std::expected`.

```cpp
struct Row { int64_t id; std::string username; };

auto list = co_await txn.query_reflect_expected<Row>(
    "SELECT id, name AS username FROM users ORDER BY id");
if (!list) {
    const auto& e = list.error();
    // e.code, e.error, e.err_detail.sqlstate
}

auto one = co_await txn.query_reflect_expected_one<Row>(
    "SELECT id, name AS username FROM users WHERE id=$1", 1);
if (!one) {
    // e.error == "no rows" если SELECT ничего не вернул
}
```

Aliases:

```cpp
auto list2 = co_await txn.select_reflect_expected<Row>(
    "SELECT id, name AS username FROM users ORDER BY id");

auto one2 = co_await txn.select_one_reflect_expected<Row>(
    "SELECT id, name AS username FROM users WHERE id=$1", 1);
```

DML с `QueryResult`:

```cpp
struct Patch { std::string name; int64_t id; };
auto upd = co_await txn.exec_reflect(
    "UPDATE users SET name=$1 WHERE id=$2", Patch{"alice", 5});
if (!upd.ok) { /* upd.error, upd.err_detail */ }
```

### Legacy reflect API (**Deprecated**)

| Method                           | Return             | Status     |
|----------------------------------|--------------------|------------|
| `query_reflect<T>(sql, ...)`     | `std::vector<T>`   | Deprecated |
| `query_reflect_one<T>(sql, ...)` | `std::optional<T>` | Deprecated |
| `select_reflect<T>(...)`         | `std::vector<T>`   | Deprecated |
| `select_one_reflect<T>(...)`     | `std::optional<T>` | Deprecated |

Use `*_expected` instead of them.

---

## Subtransactions (SAVEPOINT)

```cpp
auto sub = txn.make_subtx();
if (co_await sub.begin()) {
    struct Row { int id; int v; };
    auto r = co_await sub.query_reflect_expected<Row>(
        "UPDATE t SET v=$1 WHERE id=$2 RETURNING id, v", 42, 5);
    if (!r) co_await sub.rollback();
    else    co_await sub.commit();
}
```

Semantics:

| Method       | SQL issued                     |
|--------------|--------------------------------|
| `begin()`    | `SAVEPOINT <name>`             |
| `commit()`   | `RELEASE SAVEPOINT <name>`     |
| `rollback()` | `ROLLBACK TO SAVEPOINT <name>` |

---

## Pipeline mode

```cpp
auto r1 = co_await txn.query<true>("INSERT INTO logs(msg) VALUES($1)", "A");
auto r2 = co_await txn.query<true>("INSERT INTO logs(msg) VALUES($1)", "B");
```

* `<true>` queues and batches `PQsendQueryParams`; results consumed after sync.
* Use only for independent statements.

---

## Commit / Rollback / Finish

```cpp
bool ok = co_await txn.commit();
if (!ok) { /* txn.last_commit_error() says why */ }

co_await txn.rollback(); // explicit rollback
co_await txn.finish();   // rollback if active, then cleanup
```

`commit()` returns `false` when a statement inside the transaction already failed: PostgreSQL answers such a
`COMMIT` with `ROLLBACK`, which libpq reports as success. `last_commit_error()` is then
`{ServerError, sqlstate 25P02}`.

---

## Retrying a whole transaction: `run_tx`

```cpp
auto r = co_await usub::pg::run_tx(pool, [&](usub::pg::PgTransaction &tx)
        -> usub::uvent::task::Awaitable<std::expected<Row, usub::pg::PgOpError>> {
    auto rows = co_await tx.query_reflect_expected_one<Row>("SELECT ... FOR UPDATE", id);
    if (!rows) co_return std::unexpected(rows.error());
    auto upd = co_await tx.query("UPDATE ...", id);
    if (!upd.ok) co_return std::unexpected(usub::pg::detail::to_op_error(upd));
    co_return *rows;
}, usub::pg::QueryOpts{.io_deadline_ms = 3000, .retry_budget_ms = 10000},
   usub::pg::PgTransactionConfig{.isolation = usub::pg::TxIsolationLevel::ReadCommitted});
```

`run_tx` begins, awaits the body, commits; when the attempt failed in a way that left nothing committed it
starts over on a fresh connection:

* `NotStarted`, `ConnectionLost`, `Deadline`, `RolledBack` during BEGIN or the body (the server rolled the
  transaction back);
* `RolledBack` at COMMIT (serialization failure);
* `ConnectionLost` / `Deadline` at COMMIT only with `QueryOpts::idempotent`: the commit may have happened.

The body may run several times, so it must touch nothing but the database and must not commit or roll back
itself. Returning an error rolls back and is returned as is (business errors are not retried); an exception
rolls back and propagates. Lambda captures are fine: the body is awaited in place.

---

## Error model

* No exceptions; `QueryResult` for low-level calls.
* `*_expected` returns `std::expected<..., PgOpError>`.
* Broken connections invalidate the transaction and are retired automatically.