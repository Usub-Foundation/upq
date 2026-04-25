# 🧩 upq: Asynchronous PostgreSQL integration for uvent

### Overview

`upq` is a coroutine-based, fully asynchronous PostgreSQL client built on top of **libpq** and integrated directly into
the **uvent** event loop.  
It provides non-blocking query interfaces, connection pooling, parameterized statements, RAII-based transactions,  
and now — full **reflection-based parameter binding and mapping**
with [ureflect](https://github.com/Usub-development/ureflect).

---

### ✳️ Features

* Asynchronous connect via `PQconnectStart` + `PQconnectPoll`
* Coroutine-awaitable queries (`co_await pool.query_awaitable(...)`)
* Global static connection pool (`PgPool::instance()`)
* Lock-free MPMC queue for connection reuse
* Safe transactional RAII wrapper (`PgTransaction`)
* Parameter binding with `$1, $2, …`
* Zero-copy non-blocking I/O pipeline
* **Reflection-aware queries** (`query_reflect`, `exec_reflect`, `query_reflect_one`)  
  → automatic struct ↔ SQL row mapping via `ureflect`
* Large-export API — cursor-backed `PgRowStream`,
  streaming `COPY TO` / `COPY FROM` with async sinks/sources,
  `pg_export_snapshot` support for parallel consistent dumps
  ([see docs](docs/large-exports.md))

---

### ⚙️ Example Usage

```cpp
#include "PgPool.h"
#include "uvent/Uvent.h"

using namespace usub::uvent;

task::Awaitable<void> example()
{
    // Schema creation
    co_await usub::pg::PgPool::instance().query_awaitable(R"SQL(
        CREATE TABLE IF NOT EXISTS users(
            id SERIAL PRIMARY KEY,
            name TEXT,
            password TEXT
        );
    )SQL");

    // Transaction example
    {
        usub::pg::PgTransaction txn;
        co_await txn.begin();

        auto upd = co_await txn.query(
            "UPDATE users SET name=$1 WHERE id=$2 RETURNING name;",
            "John", 1
        );

        if (upd.ok && !upd.rows.empty())
            spdlog::info("Updated name={}", upd.rows[0].cols[0]);

        co_await txn.commit();
    }

    // Regular SELECT
    auto res = co_await usub::pg::PgPool::instance().query_awaitable(
        "SELECT id, name FROM users ORDER BY id LIMIT $1;", 5
    );

    if (res.ok)
        for (auto& r : res.rows)
            ulog::info("user: id={}, name={}", r.cols[0], r.cols[1]);

    co_return;
}
```

---

### 🪞 Reflection Integration

`upq` now supports **full reflection-based parameter binding and result mapping** powered by `ureflect`.
This eliminates manual serialization or tuple unpacking — you can send and receive plain C++ structs directly.

```cpp
struct NewUser {
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

struct UserRow {
    int64_t id;
    std::string username;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

// insert from struct
NewUser u{"Alice", std::nullopt, {1, 2, 5}, {"admin", "core"}};
co_await pool.exec_reflect(
    "INSERT INTO users_reflect(name,password,roles,tags) VALUES($1,$2,$3,$4);",
    u
);

// select into vector<UserRow>
auto users = co_await pool.query_reflect<UserRow>(
    "SELECT id, name AS username, password, roles, tags FROM users_reflect ORDER BY id;"
);

for (auto& r : users)
    spdlog::info("User {}: roles={}, tags={}", r.username, r.roles.size(), r.tags.size());

// select one row
auto one = co_await pool.query_reflect_one<UserRow>(
    "SELECT id, name AS username, password, roles, tags FROM users_reflect WHERE id=$1 LIMIT 1;",
    1
);
if (one) spdlog::info("Found user: {}", one->username);
```

**✅ Supported types**

* Aggregates (`struct`/`class`) via `ureflect`
* `std::tuple`, `std::pair`
* `std::vector`, `std::array`, native C arrays
* `std::optional<T>` ↔ `NULL`
* Scalars, strings, numeric types

**✅ Supported operations**

* `query_reflect<T>` → returns `std::vector<T>`
* `query_reflect_one<T>` → returns `std::optional<T>`
* `exec_reflect(obj)` → executes parameterized statement using reflected fields

**🧠 Matching Rules**

* Field-to-column mapping is by name (SQL aliases supported, e.g. `AS username`)
* Arrays map to PostgreSQL arrays (`INT4[]`, `TEXT[]`, etc.)
* Missing/extra columns are ignored safely
* Fully compile-time, no runtime reflection

---

### 🔩 Components

| Component           | Description                                                           |
|---------------------|-----------------------------------------------------------------------|
| `PgPool`            | Global async connection pool built on `libpq` and `uvent::MPMCQueue`. |
| `PgConnectionLibpq` | Wraps a single `PGconn`, manages non-blocking I/O.                    |
| `PgTransaction`     | RAII wrapper for BEGIN / COMMIT / ROLLBACK.                           |
| `PgReflect`         | Header-only reflection bridge for struct↔SQL mapping.                 |
| `QueryResult`       | Result container with `rows`, `cols`, `ok`, and `error`.              |

---

### 🧱 Example Schema

```sql
CREATE TABLE IF NOT EXISTS public.users_reflect (
    id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL,
    password TEXT, roles INT4[] NOT NULL,
    tags TEXT [] NOT NULL
);
```

---

### 🚀 Build

Requires:

* C++23 (clang ≥16 / gcc ≥13)
* `libpq`
* `uvent`

```cmake
include(FetchContent)

FetchContent_Declare(
        upq
        GIT_REPOSITORY https://github.com/Usub-development/upq.git
        GIT_TAG main
        OVERRIDE_FIND_PACKAGE
)

FetchContent_MakeAvailable(upq)

target_link_libraries(${PROJECT_NAME} PRIVATE usub::upq)
```

---

### 📜 License

MIT License © Usub-development