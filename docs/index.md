# upq

`upq` is an asynchronous PostgreSQL client and connection pool for modern C++23.  
It’s designed for **uvent**, fully coroutine-based, and built directly on top of **libpq** without libpqxx or any
blocking calls.

---

## Design Goals

- Fully non-blocking I/O integrated with `uvent`
- No background threads or runtime schedulers
- Coroutine-native query interface (`co_await`)
- Clear layering: pool → connection → transaction
- Minimal allocations and zero unnecessary copies
- **Compile-time reflection** for type-safe query/parameter mapping

---

## Components

| Component             | Purpose                                                  |
|-----------------------|----------------------------------------------------------|
| **PgPool**            | Global async connection pool (`PGconn` management)       |
| **PgConnectionLibpq** | Non-blocking wrapper over `libpq` I/O and protocol state |
| **PgTransaction**     | Transactional wrapper on pinned pooled connection        |
| **QueryResult**       | Lightweight structured query result                      |
| **PgReflect**         | Header-only reflection bridge for struct ↔ SQL mapping   |

---

## Features

- **Reflect-aware SELECT / EXEC** via [ureflect](https://github.com/Usub-development/ureflect)
    - `query_reflect<T>(sql, ...)` → returns `std::vector<T>`
    - `query_reflect_one<T>(sql, ...)` → returns `std::optional<T>`
    - `exec_reflect(sql, obj)` → uses struct or tuple fields as parameters
- **Name-based mapping**  
  Struct fields are matched to SQL columns **by name** (aliases like `AS username` supported).  
  Falls back to positional order if names are unavailable.
- **Array and optional support**
    - `std::optional<T>` ↔ `NULL`
    - `std::vector<T>`, `std::array<T,N>`, C arrays ↔ PostgreSQL arrays (`INT4[]`, `TEXT[]`, …)
- **No hidden layers** — stays close to raw `libpq`, but coroutine-safe and zero-overhead.
- **Async everywhere** — `connect`, `query`, `commit`, `LISTEN/NOTIFY`, `COPY`, all awaitable.

---

### Example

```cpp
struct User
{
    int64_t id;
    std::string username;
    std::optional<std::string> password;
    std::vector<int> roles;
    std::vector<std::string> tags;
};

// SELECT with name-based mapping
auto users = co_await pool.query_reflect<User>(
    "SELECT id, name AS username, password, roles, tags FROM users ORDER BY id LIMIT 100;"
);

// INSERT from aggregate
struct NewUser
{
    std::string name;
    std::optional<std::string> password;
    std::vector<int> roles;
};
NewUser nu{"alice", std::nullopt, {1, 2}};

auto res = co_await pool.exec_reflect(
    "INSERT INTO users(name, password, roles) VALUES ($1,$2,$3);",
    nu
);
```

---

## What it *doesn’t* do

* No ORM, no hidden state machines
* No query builders or migrations
* No blind retries: only failures that prove a re-run safe are retried (`RetryClass`, [pool.md](pool.md))
* No dependencies beyond `libpq` and `uvent`

**Philosophy:** minimal abstractions, predictable control, and **compile-time reflection instead of boilerplate.**