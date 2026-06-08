# JSON

UPQ supports:

- writing C++ structs into `JSONB/JSON` via query parameters
- reading `JSONB/JSON` into C++ structs via `PgJson<T, Strict>`
- strict vs non-strict parsing (unknown keys, etc.)

All JSON (de)serialization funnels through a single, swappable codec —
`usub::pg::json::dump` / `usub::pg::json::parse` (see
[`include/upq/PgJsonCodec.h`](../include/upq/PgJsonCodec.h)). The default
engine is **ujson** and is the only hard dependency; you can switch the whole
library to **glaze** or **nlohmann/json**, or override serialization for a
single type, without touching UPQ's source. See
[Pluggable JSON backend](#pluggable-json-backend) below.

---

## Pluggable JSON backend

Everything in UPQ that touches JSON goes through two functions:

```cpp
std::string                     usub::pg::json::dump(const T& v);
std::expected<T, std::string>   usub::pg::json::parse<T, Strict>(std::string_view);
```

There are three independent ways to customize them, all resolved at compile time:

1. pick a **built-in** backend (ujson / glaze / nlohmann)
2. plug in **your own** backend, entirely from code
3. **override a single type** with `custom_codec<T>`

And the choice can be driven **from CMake or from code** — see
[Switching the backend from code](#switching-the-backend-from-code-no-cmake).

### 1. Pick a global backend (works out of the box, no per-type code)

Selection is **compile-time**. The third-party header is `#include`d **only**
when its backend is selected, so glaze / nlohmann are never a hard dependency —
you only need them on the include path if you opt in.

| Backend         | Macro                        | Works out of the box for plain structs?                    |
|-----------------|------------------------------|------------------------------------------------------------|
| `ujson` (default) | *(none)*                   | Yes (ureflect aggregate reflection)                        |
| `glaze`         | `UPQ_JSON_BACKEND_GLAZE`     | Yes (glaze compile-time reflection)                        |
| `nlohmann/json` | `UPQ_JSON_BACKEND_NLOHMANN`  | Via nlohmann conventions (`NLOHMANN_DEFINE_TYPE_*` / ADL)  |

With CMake:

```bash
cmake -DUPQ_JSON_BACKEND=ujson      # default
cmake -DUPQ_JSON_BACKEND=glaze      # find_package(glaze) + link glaze::glaze
cmake -DUPQ_JSON_BACKEND=nlohmann   # find_package(nlohmann_json) + link it
```

`UPQ_JSON_BACKEND=glaze|nlohmann` will `find_package(...)` that library and
link it `PUBLIC`; ujson stays the default and nothing extra is pulled in
otherwise. You can also define the macro directly (e.g. `-DUPQ_JSON_BACKEND_GLAZE`).

`Strict` (from `PgJson<T, Strict>`) maps to unknown-key rejection: ujson and
glaze enforce it; nlohmann has no such notion, so `Strict` is accepted but not
enforced there.

> nlohmann note: nlohmann only (de)serializes types that follow its own
> conventions (`NLOHMANN_DEFINE_TYPE_INTRUSIVE`, `to_json`/`from_json`, etc.),
> and does not handle `std::optional<T>::from_json` by itself — add the usual
> `adl_serializer<std::optional<T>>` if your structs use it.

### Switching the backend from code (no CMake)

CMake's `-DUPQ_JSON_BACKEND=...` is only a convenience that sets the macros
above. You can select the backend **purely from C++**, without touching the
build system, with an auto-discovered config header.

Drop a header named **`upq_json_config.h`** anywhere on your include path. UPQ
includes it (identically in every translation unit, *before* it resolves the
backend), so whatever you `#define` there wins:

```cpp
// upq_json_config.h  — found via __has_include, no -D needed
#pragma once
#define UPQ_JSON_BACKEND_GLAZE 1
```

Prefer a different name/location? Point `UPQ_JSON_CONFIG_HEADER` at it instead
(e.g. `-DUPQ_JSON_CONFIG_HEADER="\"my/json_cfg.h\""`, or define it before the
first UPQ include).

> Because the choice is compile-time, it must be **consistent across all
> translation units** (same as any ODR-sensitive config). The config-header
> approach guarantees that automatically; defining the macro ad-hoc in a single
> `.cpp` does not.

### Plug in your own backend (any JSON library, from code)

Not limited to the three built-ins — point UPQ at a backend type you define.
No edits to `PgJsonCodec.h`, no CMake. A backend is just a type with three
members mirroring the built-ins:

```cpp
// in upq_json_config.h (or any header seen before UPQ's)
namespace my {
struct JsonBackend {
    static constexpr std::string_view name = "my";

    template <class T>
    static std::string dump(const T& v);                       // C++ -> JSON text

    template <class T, bool Strict>
    static std::expected<T, std::string> parse(std::string_view sv);  // JSON -> C++
};
}
#define UPQ_JSON_BACKEND_CUSTOM ::my::JsonBackend
```

`UPQ_JSON_BACKEND_CUSTOM` takes priority over the built-in backends. Your
backend owns its own `#include`s, so this is the seam for wiring in simdjson,
RapidJSON, Boost.JSON, a hand-rolled serializer, or even a runtime dispatcher.

### Generic / dynamic JSON (DOM)

For schemaless columns you don't want to model as a struct, use the backend's
generic JSON value as the `T` in `PgJson<T>` / `pg_jsonb(T)`. It flows through
the codec unchanged, so reading/writing arbitrary JSON works out of the box:

| Backend       | Generic value type                                  |
|---------------|-----------------------------------------------------|
| glaze         | `glz::generic` (a.k.a. deprecated `glz::json_t`)    |
| nlohmann/json | `nlohmann::json`                                    |
| ujson         | — (reflection-only; no DOM type)                    |

```cpp
// glaze backend: read a dynamic JSONB column into a DOM, then write it back
struct Row {
    int64_t id{};
    usub::pg::PgJson<glz::generic> doc;     // any JSON shape
};

glz::generic d;
d["tags"] = glz::generic::array_t{};
auto ins = co_await pool.exec_reflect(
    "INSERT INTO t(doc) VALUES($1)", std::tuple{usub::pg::pg_jsonb(d)});
```

`glz::generic` is available straight from `<glaze/glaze.hpp>` (which the codec
already includes); no extra header is needed.

### 2. Override a single type (wins over any global backend)

Specialize `usub::pg::json::custom_codec<T>` to control one type regardless of
which backend is selected. A per-type override always wins:

```cpp
namespace usub::pg::json {
    template <>
    struct custom_codec<MyType> {
        static std::string dump(const MyType& v);
        static std::expected<MyType, std::string>
        parse(std::string_view sv, bool strict);
    };
}
```

Use this when one type needs hand-rolled JSON (e.g. a domain wrapper) while the
rest of the codebase keeps using the global backend.

---

## Types

### PgJson

`PgJson<T, Strict>` is a result-field wrapper decoded from JSON text.

```cpp
template<class T, bool Strict = true>
struct PgJson {
    static constexpr bool strict = Strict;
    T value{};
};
```

* `Strict=true`: unknown keys => parse error
* `Strict=false`: unknown keys are ignored (as implemented by ujson)

### PgJsonParam

`PgJsonParam<T, Strict, Jsonb>` is a parameter wrapper used to serialize an object into JSON for `INSERT/UPDATE`.

Helpers:

```cpp
template<class T, bool Strict = true>
PgJsonParam<T, Strict, true>  pg_jsonb(const T& v);

template<class T, bool Strict = true>
PgJsonParam<T, Strict, false> pg_json (const T& v);
```

Use `pg_jsonb()` in almost all cases.

## Serialization (C++ → JSON/JSONB)

UPQ serializes C++ objects into JSON by calling `usub::pg::json::dump(obj)` (the active backend, ujson by default)
inside the parameter encoder and sends the result as a typed parameter:

- `pg_jsonb(obj)` → parameter type `JSONBOID` (JSONB)
- `pg_json(obj)`  → parameter type `JSONOID`  (JSON)

### Serialize a struct into JSONB (recommended)

```cpp
Profile p{ .age = 27, .city = std::string("AMS"), .flags = {"a","b"} };

auto r = co_await pool.exec_reflect(
    "INSERT INTO users_json_demo(username, profile) VALUES($1,$2)",
    std::tuple{std::string("kirill"), usub::pg::pg_jsonb(p)}
);

if (!r.ok) std::cerr << r.error << "\n";
````

### Serialize into JSON (not JSONB)

```cpp
Profile p{ .age = 1, .city = std::nullopt, .flags = {"x"} };

auto r = co_await pool.exec_reflect(
    "INSERT INTO users_json_demo(username, profile_json) VALUES($1,$2)",
    std::tuple{std::string("json"), usub::pg::pg_json(p)}
);
```

### Bind JSON parameters without tuples

If your `exec_reflect` supports direct argument passing, this is equivalent:

```cpp
Profile p{ .age = 27, .city = std::string("AMS"), .flags = {"a","b"} };

auto r = co_await pool.exec_reflect(
    "INSERT INTO users_json_demo(username, profile) VALUES($1,$2)",
    "kirill", usub::pg::pg_jsonb(p)
);
```

### Important details

* Serialization uses `usub::pg::json::dump()` (the selected backend; ujson by default).
* The parameter is sent as **text format** with an explicit OID (`JSONBOID` / `JSONOID`).
* `Strict` in `PgJsonParam<T, Strict, ...>` currently does **not** change serialization; it matters on **decode** (
  `PgJson<T, Strict>`).
* If you insert broken JSON from SQL directly, Postgres will accept it as valid JSONB, but strict decoding may fail
  later.

### Custom JSON text (pre-serialized)

If you already have JSON text, just pass it as `std::string` and cast in SQL:

```cpp
std::string raw = R"({"age":1,"city":"A","flags":["x"]})";

auto r = co_await pool.exec_reflect(
    "INSERT INTO users_json_demo(username, profile) VALUES($1, $2::jsonb)",
    std::tuple{std::string("raw"), raw}
);
```

This bypasses struct serialization.

---

## ujson enum mapping (enum_meta)

If your JSON contains enums, define mapping in ujson:

```cpp
enum class Role { User, Admin };

namespace ujson {
template<>
struct enum_meta<Role> {
    static inline constexpr auto items = enumerate<Role::User, Role::Admin>();
};
} // namespace ujson
```

---

## Example: write and read JSONB with PgPool

This example:

* creates a table
* inserts a valid `Profile` using `exec_reflect + pg_jsonb(Profile)`
* inserts a “broken” JSONB payload with an extra `UNKNOWN` key
* reads valid rows with strict parsing
* shows that strict parsing fails on the broken row
* reads the broken row with non-strict parsing successfully

### Models

```cpp
struct Profile {
    int age{};
    std::optional<std::string> city;
    std::vector<std::string> flags;
};

struct UserJsonRowStrict {
    int64_t id{};
    std::string username;
    usub::pg::PgJson<Profile, true> profile;   // strict
};

struct UserJsonRowLoose {
    int64_t id{};
    std::string username;
    usub::pg::PgJson<Profile, false> profile;  // non-strict
};
```

### Full coroutine demo

```cpp
usub::uvent::task::Awaitable<void> demo_pgjson_ujson(usub::pg::PgPool& pool) {
    using namespace usub::pg;

    std::cout.setf(std::ios::unitbuf);
    std::cout << "[JSON] demo start\n";

    // 1) schema
    {
        auto r = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS users_json_demo (
                id        BIGSERIAL PRIMARY KEY,
                username  TEXT  NOT NULL,
                profile   JSONB NOT NULL
            );
        )SQL");
        if (!r.ok) { std::cerr << "[JSON/SCHEMA] " << r.error << "\n"; co_return; }

        auto t = co_await pool.query_awaitable("TRUNCATE users_json_demo RESTART IDENTITY");
        if (!t.ok) { std::cerr << "[JSON/TRUNCATE] " << t.error << "\n"; co_return; }

        std::cout << "[JSON/SCHEMA+TRUNCATE] OK\n";
    }

    // 2) insert good (C++ -> JSONB)
    {
        Profile p{ .age = 27, .city = std::string("AMS"), .flags = {"a","b"} };
        std::string name = "kirill";

        auto ins = co_await pool.exec_reflect(
            "INSERT INTO users_json_demo(username, profile) VALUES($1,$2)",
            std::tuple{name, pg_jsonb(p)}
        );
        if (!ins.ok) { std::cerr << "[JSON/INSERT good] " << ins.error << "\n"; co_return; }
        std::cout << "[JSON/INSERT good] OK\n";
    }

    // 3) insert broken (extra key)
    {
        auto ins = co_await pool.query_awaitable(R"SQL(
            INSERT INTO users_json_demo(username, profile)
            VALUES ('broken', '{"age":1,"city":"A","flags":["x"],"UNKNOWN":123}'::jsonb);
        )SQL");
        if (!ins.ok) { std::cerr << "[JSON/INSERT broken] " << ins.error << "\n"; co_return; }
        std::cout << "[JSON/INSERT broken] OK\n";
    }

    // 4) strict read: read only valid rows
    {
        auto rows = co_await pool.query_reflect_expected<UserJsonRowStrict>(
            "SELECT id, username, profile FROM users_json_demo "
            "WHERE username <> 'broken' ORDER BY id"
        );

        if (!rows) {
            std::cerr << "[JSON/SELECT strict good] FAIL code=" << toString(rows.error().code)
                      << " err='" << rows.error().error << "'\n";
        } else {
            std::cout << "[JSON/SELECT strict good] OK n=" << rows->size() << "\n";
            for (auto& r : *rows) {
                std::cout << "  id=" << r.id
                          << " username=" << r.username
                          << " age=" << r.profile.value.age
                          << " city=" << (r.profile.value.city ? *r.profile.value.city : "<NULL>")
                          << " flags=" << r.profile.value.flags.size()
                          << "\n";
            }
        }
    }

    // 5) strict read: broken row should fail
    {
        auto rows = co_await pool.query_reflect_expected<UserJsonRowStrict>(
            "SELECT id, username, profile FROM users_json_demo WHERE username='broken' LIMIT 1"
        );

        if (!rows) {
            std::cout << "[JSON/SELECT strict broken] EXPECTED FAIL code=" << toString(rows.error().code)
                      << " err='" << rows.error().error << "'\n";
        } else {
            std::cout << "[JSON/SELECT strict broken] UNEXPECTED OK\n";
        }
    }

    // 6) loose read: broken row is accepted
    {
        auto rows = co_await pool.query_reflect_expected<UserJsonRowLoose>(
            "SELECT id, username, profile FROM users_json_demo WHERE username='broken' LIMIT 1"
        );

        if (!rows) {
            std::cerr << "[JSON/SELECT loose] FAIL code=" << toString(rows.error().code)
                      << " err='" << rows.error().error << "'\n";
            co_return;
        }

        std::cout << "[JSON/SELECT loose] OK n=" << rows->size() << "\n";
        for (auto& r : *rows) {
            std::cout << "  id=" << r.id
                      << " username=" << r.username
                      << " age=" << r.profile.value.age
                      << " city=" << (r.profile.value.city ? *r.profile.value.city : "<NULL>")
                      << " flags=" << r.profile.value.flags.size()
                      << "\n";
        }
    }

    std::cout << "[JSON] demo done\n";
    co_return;
}
```

## Notes on debug logs

If you see logs like:

* `fields: [age] [city] [flags]`
* `key='UNKNOWN' ...`

that is your **ujson/reflect debug output** for strict parsing on a payload containing an unknown key. It’s expected
while strict parsing is failing.

To silence it, disable your debug macro(s) in ujson / reflect layer (whatever emits those prints), or gate them behind a
compile-time flag.