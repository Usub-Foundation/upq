# 📦 Large exports & bulk loads

`upq` provides Go-inspired streaming primitives for moving large volumes of
data in and out of PostgreSQL without buffering entire result sets in memory.

The design maps directly onto patterns familiar from Go's `pgx`:

| Go (`pgx`)                        | upq                                            |
|-----------------------------------|------------------------------------------------|
| `conn.Query(...)` + `rows.Next()` | `pool.stream(...)` → `PgRowStream::next()`     |
| `conn.CopyTo(writer, sql)`        | `pool.copy_to(sql, sink)`                      |
| `conn.CopyFrom(...)`              | `pool.copy_from(sql, source)`                  |
| `pg_export_snapshot` + workers    | `tx.export_snapshot()` / `tx.set_snapshot(id)` |

All operations are coroutine-based — they never block the event loop, memory
use is O(chunk/batch), and backpressure is naturally expressed by awaiting
the sink/source.

---

## 1. `PgRowStream` — streaming row iterator

Equivalent to Go's `rows.Next()`. Backed by a server-side cursor; only
`batch_size` rows live in memory at any time.

```cpp
#include "upq/PgPool.h"
#include "upq/PgRowStream.h"

task::Awaitable<void> export_users(PgPool& pool) {
    auto sres = co_await pool.stream(
        "SELECT id, name FROM users WHERE active ORDER BY id", 5000);
    if (!sres) {
        // handle sres.error()
        co_return;
    }
    auto& s = *sres;

    while (auto row = co_await s.next()) {
        // row->cols[0], row->cols[1] — std::optional<std::string>
        process(*row);
    }

    if (!s.ok()) {
        // mid-stream error; s.error() is populated
    }
    co_await s.close();   // commits outer tx & returns conn to pool
}
```

The `batch_size` parameter controls how many rows are fetched per round-trip
(via `FETCH FORWARD N`). Larger batches = fewer round-trips; smaller batches
= lower latency per row and lower peak memory.

### Typed variant

```cpp
struct UserRow {
    int64_t id;
    std::string name;
};

auto sres = co_await pool.stream_reflect<UserRow>(
    "SELECT id, name FROM users ORDER BY id", 5000);
if (!sres) co_return;

auto& s = *sres;
while (auto u = co_await s.next()) {
    spdlog::info("{} {}", u->id, u->name);
}
co_await s.close();
```

Fields are matched to columns by name (same rules as `query_reflect`), with
positional fallback.

### Using inside an existing transaction

```cpp
PgTransaction tx(&pool, {.isolation = TxIsolationLevel::RepeatableRead});
if (!co_await tx.begin()) co_return;

auto sres = co_await tx.stream("SELECT ...");
// ... iterate ...
co_await sres->close();  // only closes the cursor; tx stays open

co_await tx.commit();
```

The stream returned from a transaction does **not** commit on `close()` —
lifetime is bound to `tx`.

### Bind parameters (`$1, $2, …`)

`stream` and `stream_reflect` accept variadic arguments after `batch_size`,
forwarded to the cursor's underlying `SELECT` via libpq's extended protocol
(`PQsendQueryParams`). Postgres binds them inside the `DECLARE … CURSOR FOR …`
in a single round trip — no manual `PREPARE`, no string interpolation.

```cpp
const int64_t lower = 100000;
const int64_t upper = 100100;

auto sres = co_await pool.stream_reflect<BigRow>(
    "SELECT id, name, tag FROM big_table "
    "WHERE id BETWEEN $1 AND $2 ORDER BY id",
    1000, lower, upper);
```

The first non-SQL argument is `batch_size` (required, no default in this
overload — pass `1000` or whatever you'd otherwise default to). Everything
after it is passed positionally as `$1`, `$2`, …. Argument count is checked
at runtime via `assert` against `count_pg_params(sql)` — same path the
existing `query_awaitable(sql, args...)` uses.

Mixing with the pipeline works the same as without parameters — the
returned stream is still a normal `PgRowStream` / `PgTypedRowStream<T>`:

```cpp
namespace ps = usub::pg::stream;

auto s = co_await pool.stream_reflect<BigRow>(
    "SELECT id, name, tag FROM big_table "
    "WHERE id > $1 AND tag IS NOT NULL ORDER BY id",
    2000, int64_t{50000});

int64_t sum = co_await(
    std::move(*s)
    | ps::filter([](const BigRow& r) { return r.id % 2 == 0; })
    | ps::take(1000)
    | ps::reduce(int64_t{0}, [](int64_t acc, BigRow r) { return acc + r.id; })
);
```

The same templated overloads are also on `PgTransaction::stream` and
`PgTransaction::stream_reflect`.

#### What about `copy_to` / `copy_from`?

`COPY tablename TO STDOUT` does not accept parameters in its grammar, and
`COPY (SELECT ... WHERE x = $1) TO STDOUT` cannot be combined cleanly with
libpq's extended-protocol path because libpq does not switch into COPY mode
after `PQsendQueryParams`. If you need a parameterised export, use
`pool.stream(sql, batch_size, args…)` instead — the row-by-row path is the
intended substitute. `COPY FROM STDIN` has no place for parameters at all,
so `copy_from` / `copy_from_buffer` only take SQL.

---

## 2. `copy_to` — streaming COPY OUT

Equivalent to Go's `conn.CopyTo(writer, sql)`. The server streams CSV / TEXT /
BINARY COPY data; upq hands each chunk to your async sink.

```cpp
#include <fstream>

task::Awaitable<void> dump_to_csv(PgPool& pool, const std::string& path) {
    std::ofstream out(path, std::ios::binary);

    auto sink = [&out](std::string_view chunk) -> task::Awaitable<bool> {
        out.write(chunk.data(), chunk.size());
        co_return out.good();   // false aborts the COPY
    };

    PgCopyResult r = co_await pool.copy_to(
        "COPY (SELECT id, name, created_at FROM big_table) "
        "TO STDOUT WITH (FORMAT csv, HEADER)",
        sink);

    if (!r.ok) {
        // r.error / r.err_detail
    }
}
```

The sink is called once per COPY data chunk; libpq usually emits one chunk
per row but the protocol does not guarantee it, so for line-oriented
processing use `copy_to_lines`.

Returning `false` from the sink aborts cleanly — the rest of the stream is
drained so the connection stays reusable.

### Piping to gzip / S3 multipart / network

The sink is just `(string_view) -> Awaitable<bool>`. Compose freely:

```cpp
GzipWriter gz(output_stream);
auto sink = [&gz](std::string_view c) -> task::Awaitable<bool> {
    co_return co_await gz.write_async(c);
};

co_await pool.copy_to("COPY (...) TO STDOUT WITH (FORMAT csv)", sink);
```

### `copy_to_lines` — line-oriented

Handy for CSV/TEXT exports where you want to process one record at a time
without worrying that a chunk boundary lands mid-row:

```cpp
uint64_t counted = 0;
auto on_line = [&](std::string_view line) -> task::Awaitable<bool> {
    if (!line.empty()) ++counted;
    co_return true;
};
co_await pool.copy_to_lines("COPY (...) TO STDOUT WITH (FORMAT csv)", on_line);
```

Chunks that split a row are joined transparently across calls.

---

## 3. `copy_from` — streaming COPY IN

Equivalent to Go's `conn.CopyFrom`. The caller supplies an async pull
source; upq feeds it into a `COPY ... FROM STDIN` on the server.

```cpp
task::Awaitable<void> bulk_load(PgPool& pool, std::istream& src) {
    char buf[64 * 1024];

    auto source = [&]() -> task::Awaitable<std::string_view> {
        src.read(buf, sizeof(buf));
        const auto n = src.gcount();
        if (n <= 0) co_return std::string_view{};  // EOF
        co_return std::string_view(buf, static_cast<size_t>(n));
    };

    PgCopyResult r = co_await pool.copy_from(
        "COPY landing(id, name, ts) FROM STDIN WITH (FORMAT csv)",
        source);

    if (r.ok) {
        spdlog::info("inserted {} rows", r.rows_affected);
    }
}
```

Returning an empty `string_view` from the source signals end-of-data and
triggers `COPY ... DONE`. The source's buffer must remain valid until the
next call — upq copies the chunk into libpq's send buffer during the call
but relies on the view being stable for its duration.

### One-shot variant

```cpp
std::string payload = build_csv();
auto r = co_await pool.copy_from_buffer(
    "COPY landing FROM STDIN WITH (FORMAT csv)",
    payload.data(), payload.size());
```

---

## 4. Parallel workers with `pg_export_snapshot`

For very large exports, parallelise over N connections with a **consistent
snapshot** — every worker sees exactly the same database state, just like
`pg_dump --jobs=N`.

```cpp
task::Awaitable<void> parallel_dump(PgPool& pool) {
    // Primary transaction — holds the snapshot live.
    PgTransaction leader(&pool, {.isolation = TxIsolationLevel::RepeatableRead,
                                 .read_only = true});
    if (!co_await leader.begin()) co_return;

    auto snap = co_await leader.export_snapshot();
    if (!snap) co_return;
    const std::string snap_id = *snap;

    // Launch N workers sharing that snapshot.
    std::vector<task::Awaitable<void>> tasks;
    for (int i = 0; i < 8; ++i) {
        tasks.emplace_back([&, i]() -> task::Awaitable<void> {
            PgTransaction w(&pool,
                {.isolation = TxIsolationLevel::RepeatableRead,
                 .read_only = true});
            if (!co_await w.begin()) co_return;

            // IMPORTANT: must be the first statement after BEGIN.
            if (auto err = co_await w.set_snapshot(snap_id); err) co_return;

            // Partition by id range — all 8 workers see identical data.
            auto stream = co_await w.stream_reflect<Row>(
                "SELECT ... WHERE id % 8 = $1", 5000, i);
            while (auto r = co_await stream->next()) { /* ... */ }
            co_await stream->close();
            co_await w.commit();
        }());
    }
    for (auto& t : tasks) co_await t;
    co_await leader.commit();
}
```

### Why repeatable read?

`pg_export_snapshot()` only works in `REPEATABLE READ` or `SERIALIZABLE`
transactions. All workers must use one of these levels too.

---

## 5. Keyset pagination

For resumable exports (so you can restart after crashes), iterate by primary
key instead of `OFFSET`:

```cpp
int64_t last_id = 0;
for (;;) {
    auto rows = co_await pool.query_reflect_expected<Row>(
        "SELECT id, ... FROM t WHERE id > $1 ORDER BY id LIMIT 10000",
        last_id);
    if (!rows || rows->empty()) break;
    for (const auto& r : *rows) {
        emit(r);
        last_id = r.id;
    }
    checkpoint(last_id);
}
```

Avoid `OFFSET` for large tables — it scans and discards skipped rows, giving
O(n²) total cost.

---

## 6. Ranges-style pipelines (`|`)

For fluent row processing, upq exposes a small ranges-inspired pipeline
layer in `usub::pg::stream`. It is deliberately minimal — just the
combinators that compose cleanly on top of async, move-only streams.

```cpp
#include "upq/PgStream.h"

namespace ps = usub::pg::stream;

auto sres = co_await pool.stream("SELECT id, name, tag FROM big_table");
if (!sres) co_return;

uint64_t n = co_await(
    std::move(*sres)                                   // move the stream in
    | ps::as<BigRow>                                   // reflect to typed
    | ps::filter([](const BigRow& r){ return r.tag; }) // keep tagged only
    | ps::take(50000)                                  // cap
    | ps::for_each([](BigRow r){ process(std::move(r)); })
);
```

### Available combinators

| Adaptor        | Effect                              |
|----------------|-------------------------------------|
| `transform(f)` | `T → U` (where `U = f(T)`)          |
| `filter(f)`    | `T → T`, drops when `f(t) == false` |
| `take(n)`      | first `n` elements                  |
| `skip(n)`      | drops first `n` elements            |
| `as<T>`        | `QueryResult::Row → T` (reflected)  |

### Terminals

| Terminal          | Returns                                |
|-------------------|----------------------------------------|
| `count`           | `Awaitable<uint64_t>`                  |
| `collect`         | `Awaitable<std::vector<value_type>>`   |
| `for_each(f)`     | `Awaitable<uint64_t>` (rows processed) |
| `reduce(init, f)` | `Awaitable<Acc>` — `f(acc, row) → acc` |

All terminals **close the stream** (releases the cursor and, for pool-owned
streams, returns the connection) before returning.

### Usage rules

1. **Left side of `|` must be an rvalue.** Streams are move-only; named
   variables must be `std::move`d. This matches how `std::views` works on
   move-only views.
2. **`pool.stream(...)` returns `expected<PgRowStream, PgOpError>`.**
   Unwrap before piping.
3. **`for_each(f)` accepts sync or async callables** — if `f(row)` returns
   `Awaitable<void>` it is awaited, otherwise invoked directly.

### When *not* to use pipelines

The pipe layer is sugar on top of `while (auto row = co_await s.next())`.
Reach for it when it makes code clearer — typically 3+ stages. For a
single-step loop, the direct form is fine and involves zero template
instantiations.

---

## 7. Practical notes

- **Use a read replica** for long exports. Open transactions hold the xmin
  horizon and block `VACUUM`.
- **`statement_timeout`**: may kill your long-running `DECLARE`/`FETCH` or
  `COPY`. Either raise it for the session or chunk the work with keyset
  pagination.
- **Back-pressure**: the sink/source callbacks are coroutines — if your
  downstream (file, socket, compressor) is slower than PG, upq naturally
  stops reading from the server. No unbounded memory growth.
- **Fatal vs. non-fatal errors**: on `SocketReadFailed` / `ConnectionClosed`
  the pool `mark_dead`s the connection; on server errors (e.g. syntax,
  privilege) the connection is returned intact.
- **Binary COPY**: `COPY (...) TO STDOUT WITH (FORMAT binary)` works with
  `copy_to` — your sink just receives binary bytes instead of text.

---

## 8. Performance & cost

Honest numbers. None of these are worst-case OOM scenarios — the library
has no unbounded buffers — but some choices cost more than others.

### Memory bounds

| What buffers                  | Bound                                                  |
|-------------------------------|--------------------------------------------------------|
| `PgRowStream` internal buffer | `batch_size` rows × row-size (default 1000 × row-size) |
| `copy_to` per-call chunk      | libpq's chunk size, typically ~8 KiB                   |
| `copy_to_lines` carry buffer  | length of the longest line seen so far                 |
| `copy_from` libpq send buffer | ~8 KiB before backpressure (`PQputCopyData == 0`)      |
| `collect` terminal            | **entire result set** — avoid on big tables            |

Default `batch_size=1000` at ~100 bytes/row ≈ 100 KiB peak. Safe.

### Per-row / per-chunk CPU

| Op                                      | Cost per item                  |
|-----------------------------------------|--------------------------------|
| `PgRowStream::next()` (hit buffer)      | ~0 (just index increment)      |
| `PgRowStream::next()` (fetch new batch) | 1 round-trip + batch decode    |
| `copy_to` sink invocation               | 1 chunk read + 1 sink co_await |
| `copy_from` source invocation           | 1 chunk queue (rc==0 → flush)  |

`copy_in_send_chunk` no longer flushes per chunk — libpq batches up to
~8 KiB internally and flushes only when its buffer fills or at
`copy_in_finish`. This matters for workloads that stream many small
chunks: previously ~1000 flushes/MB → now ~128 flushes/MB at most.

### Pipeline (`|`) overhead — the one that bites

Each adaptor stage (`transform`, `filter`, `take`, …) is its own coroutine
frame allocated on each `next()` call. The compiler's HALO (Heap Allocation
eLision Optimization) does not reliably fire across chained `co_await`s —
in practice you should assume **one malloc per stage per row**.

For a 5-stage pipeline over 1M rows → ~5M small allocations. On modern
glibc `malloc` (~100 ns/alloc) that's **~0.5 seconds of pure allocation
overhead** — on top of the actual work.

Rule of thumb:

| Scale | Pipeline `|`    | Direct `while` loop |
|---------------------------------|-----------------|---------------------|
| < 100k rows, any # of stages | ✅ use it | ok |
| 1M rows, 1–2 stages | ✅ fine | ok |
| 1M+ rows, 3+ stages | ⚠️ measure | ✅ prefer |
| 10M+ rows | ❌ avoid | ✅ use |

The direct form always wins because `PgRowStream::next()` reuses one
buffer and allocates a coroutine frame **only for the `next()` call
itself**, not per stage.

### COPY TO abort cost

If a `copy_to` sink returns `false` mid-stream, the current implementation
drains the rest of the result into the void to leave the connection clean
for reuse. That means:

- A 10 GB export aborted at 1% still transfers the remaining 9.9 GB over
  the network.
- Connection stays healthy — no `mark_dead`.

A future enhancement could use `PQgetCancel`+`PQcancel` to actually stop
the server. Not implemented today because cancellation uses a separate
socket and adds real complexity for a corner case.

**Recommendation:** if your sink might reject data, put the filter
upstream in SQL (`WHERE`) rather than in the sink.

### Choosing an API for throughput

From fastest to slowest for dumping a big table to a file:

1. **`pool.copy_to(sql, file_sink)`** — server does text formatting, no
   per-row round-trips, no reflection.
2. **`pool.stream(...)` + direct `while` loop** — row-level control,
   one batch round-trip per `batch_size`, no per-row mapping.
3. **`pool.stream_reflect<T>(...)` + direct `while` loop** — adds
   reflection cost per row (field name lookup + decode). Typically 10–30%
   slower than untyped streaming.
4. **Pipeline (`| as<T> | filter | ...`)** — as above + pipeline overhead.

If you're just writing rows to disk, **always prefer `copy_to`**. It's
what `pg_dump` uses for a reason.

---

## API cheat-sheet

```cpp
// Row streaming
task::Awaitable<expected<PgRowStream, PgOpError>>
    pool.stream(sql, batch_size = 1000);

template<typename... Args>
task::Awaitable<expected<PgRowStream, PgOpError>>
    pool.stream(sql, batch_size, args...);   // $1, $2, ... bound via PQsendQueryParams

template<class T>
task::Awaitable<expected<PgTypedRowStream<T>, PgOpError>>
    pool.stream_reflect<T>(sql, batch_size = 1000);

template<class T, typename... Args>
task::Awaitable<expected<PgTypedRowStream<T>, PgOpError>>
    pool.stream_reflect<T>(sql, batch_size, args...);

// COPY OUT (no parameter binding — see section 1)
template<class Sink>   // (string_view) -> Awaitable<bool>
task::Awaitable<PgCopyResult> pool.copy_to(sql, sink);

template<class LineSink>   // (string_view line) -> Awaitable<bool>
task::Awaitable<PgCopyResult> pool.copy_to_lines(sql, line_sink);

// COPY IN (no parameter binding — grammar does not allow it)
template<class Source>   // () -> Awaitable<string_view>  (empty = EOF)
task::Awaitable<PgCopyResult> pool.copy_from(sql, source);

task::Awaitable<PgCopyResult>
    pool.copy_from_buffer(sql, const void* data, size_t len);

// Snapshots (on PgTransaction)
task::Awaitable<expected<string, PgOpError>> tx.export_snapshot();
task::Awaitable<optional<PgOpError>> tx.set_snapshot(id);
```

Everything above is also available on `PgTransaction` with identical
signatures — the transaction variants use the caller's connection and never
commit implicitly.
