#include <chrono>
#include <iostream>
#include <string>
#include <vector>
#include <optional>
#include <array>
#include <list>

#include "uvent/Uvent.h"
#include "upq/PgTypes.h"
#include "upq/PgPool.h"
#include "upq/PgTransaction.h"
#include "upq/PgNotificationListener.h"
#include "upq/PgNotificationMultiplexer.h"
#include "upq/PgReflect.h"
#include "upq/PgRouting.h"
#include "upq/PgRoutingBuilder.h"

using namespace usub::uvent;

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

struct ByName {
    std::string name;
};

struct Ret {
    int64_t id;
    std::string username;
};

struct Upd {
    std::string name;
    int64_t id;
};

struct UpdRoles {
    std::vector<int> roles;
    int64_t id;
};

struct ParamRow {
    int64_t id;
    std::string name;
};

struct InsRow {
    std::string name;
    int value;
    std::string comment;
};

struct ById {
    int64_t id;
};

struct Filter {
    int min_value;
    int max_value;
};

// task::Awaitable<void> test_param_count_pool(usub::pg::PgPool &pool) {
//     std::cout << "[PARAM-COUNT/POOL] start\n";
//
//     // --- schema ---
//     {
//         auto r = co_await pool.query_awaitable(R"SQL(
//             CREATE TABLE IF NOT EXISTS param_test (
//                 id       BIGSERIAL PRIMARY KEY,
//                 name     TEXT NOT NULL,
//                 value    INT NOT NULL,
//                 comment  TEXT
//             );
//         )SQL");
//         if (!r.ok) {
//             std::cout << "[PARAM-COUNT/POOL] schema fail: " << r.error << "\n";
//             co_return;
//         }
//         co_await pool.query_awaitable("TRUNCATE param_test RESTART IDENTITY");
//     }
//
//     // --- query_awaitable: 3 placeholders, 3 args ---
//     {
//         auto r = co_await pool.query_awaitable(
//             "INSERT INTO param_test(name, value, comment) VALUES($1, $2, $3)",
//             "alice", 42, "test");
//         std::cout << "[PARAM-COUNT/POOL] query_awaitable($1,$2,$3 -> 3 args): "
//                 << (r.ok ? "OK" : r.error) << "\n";
//     }
//
//     // --- query_awaitable: 0 placeholders, 0 args ---
//     {
//         auto r = co_await pool.query_awaitable(
//             "SELECT count(1) FROM param_test");
//         std::cout << "[PARAM-COUNT/POOL] query_awaitable(0 placeholders, 0 args): "
//                 << (r.ok ? "OK" : r.error) << "\n";
//     }
//
//     // --- query_on: 1 placeholder, 1 arg ---
//     {
//         auto c = co_await pool.acquire_connection();
//         if (c && *c) {
//             auto conn = *c;
//             auto r = co_await pool.query_on(conn,
//                                             "SELECT id, name FROM param_test WHERE id = $1", int64_t{1});
//             std::cout << "[PARAM-COUNT/POOL] query_on($1 -> 1 arg): "
//                     << (r.ok ? "OK" : r.error) << "\n";
//             co_await pool.release_connection_async(conn);
//         }
//     }
//
//     // --- query_reflect_expected: 2 placeholders, 2 args ---
//     {
//         auto rows = co_await pool.query_reflect_expected<ParamRow>(
//             "SELECT id, name FROM param_test ORDER BY id LIMIT $1 OFFSET $2",
//             10, 0);
//         std::cout << "[PARAM-COUNT/POOL] query_reflect_expected($1,$2 -> 2 args): "
//                 << (rows ? "OK n=" + std::to_string(rows->size()) : "FAIL") << "\n";
//     }
//
//     // --- query_reflect_expected_one: 1 placeholder, 1 arg ---
//     {
//         auto one = co_await pool.query_reflect_expected_one<ParamRow>(
//             "SELECT id, name FROM param_test WHERE name = $1 LIMIT 1",
//             std::string("alice"));
//         std::cout << "[PARAM-COUNT/POOL] query_reflect_expected_one($1 -> 1 arg): "
//                 << (one ? "OK" : "not found") << "\n";
//     }
//
//     // --- exec_reflect: struct with 3 fields -> 3 placeholders ---
//     {
//         InsRow row{"bob", 99, "from exec_reflect"};
//         auto r = co_await pool.exec_reflect(
//             "INSERT INTO param_test(name, value, comment) VALUES($1, $2, $3)",
//             row);
//         std::cout << "[PARAM-COUNT/POOL] exec_reflect(struct 3 fields -> $1,$2,$3): "
//                 << (r.ok ? "OK" : r.error) << "\n";
//     }
//
//     // --- query_on_reflect: 1 placeholder, 1 arg ---
//     {
//         auto c = co_await pool.acquire_connection();
//         if (c && *c) {
//             auto conn = *c;
//             auto rows = co_await pool.query_on_reflect<ParamRow>(conn,
//                                                                  "SELECT id, name FROM param_test WHERE value = $1",
//                                                                  42);
//             std::cout << "[PARAM-COUNT/POOL] query_on_reflect($1 -> 1 arg): OK n="
//                     << rows.size() << "\n";
//             co_await pool.release_connection_async(conn);
//         }
//     }
//
//     // --- query_on_reflect_one: 1 placeholder, 1 arg ---
//     {
//         auto c = co_await pool.acquire_connection();
//         if (c && *c) {
//             auto conn = *c;
//             auto one = co_await pool.query_on_reflect_one<ParamRow>(conn,
//                                                                     "SELECT id, name FROM param_test WHERE name = $1 LIMIT 1",
//                                                                     std::string("bob"));
//             std::cout << "[PARAM-COUNT/POOL] query_on_reflect_one($1 -> 1 arg): "
//                     << (one ? "OK" : "not found") << "\n";
//             co_await pool.release_connection_async(conn);
//         }
//     }
//
//     // --- query_on_reflect_expected: 2 placeholders, 2 args ---
//     {
//         auto c = co_await pool.acquire_connection();
//         if (c && *c) {
//             auto conn = *c;
//             auto rows = co_await pool.query_on_reflect_expected<ParamRow>(conn,
//                                                                           "SELECT id, name FROM param_test WHERE value >= $1 ORDER BY id LIMIT $2",
//                                                                           0, 100);
//             std::cout << "[PARAM-COUNT/POOL] query_on_reflect_expected($1,$2 -> 2 args): "
//                     << (rows ? "OK n=" + std::to_string(rows->size()) : "FAIL") << "\n";
//             co_await pool.release_connection_async(conn);
//         }
//     }
//
//     // --- query_on_reflect_expected_one: 1 placeholder, 1 arg ---
//     {
//         auto c = co_await pool.acquire_connection();
//         if (c && *c) {
//             auto conn = *c;
//             auto one = co_await pool.query_on_reflect_expected_one<ParamRow>(conn,
//                                                                              "SELECT id, name FROM param_test WHERE id = $1",
//                                                                              int64_t{1});
//             std::cout << "[PARAM-COUNT/POOL] query_on_reflect_expected_one($1 -> 1 arg): "
//                     << (one ? "OK" : "not found") << "\n";
//             co_await pool.release_connection_async(conn);
//         }
//     }
//
//     std::cout << "[PARAM-COUNT/POOL] all passed\n";
//     co_return;
// }
//
// task::Awaitable<void> test_param_count_transaction(usub::pg::PgPool &pool) {
//     std::cout << "[PARAM-COUNT/TX] start\n";
//
//     // --- schema ---
//     {
//         auto r = co_await pool.query_awaitable(R"SQL(
//             CREATE TABLE IF NOT EXISTS param_test_tx (
//                 id       BIGSERIAL PRIMARY KEY,
//                 name     TEXT NOT NULL,
//                 value    INT NOT NULL,
//                 comment  TEXT
//             );
//         )SQL");
//         if (!r.ok) {
//             std::cout << "[PARAM-COUNT/TX] schema fail: " << r.error << "\n";
//             co_return;
//         }
//         co_await pool.query_awaitable("TRUNCATE param_test_tx RESTART IDENTITY");
//     }
//
//     usub::pg::PgTransaction tx(&pool);
//     if (auto err = co_await tx.begin_errored(); err) {
//         co_await tx.finish();
//         std::cout << "[PARAM-COUNT/TX] begin failed: " << err->error << "\n";
//         co_return;
//     }
//
//     // --- tx.query: 3 placeholders, 3 args ---
//     {
//         auto r = co_await tx.query(
//             "INSERT INTO param_test_tx(name, value, comment) VALUES($1, $2, $3)",
//             "alice", 10, "from tx.query");
//         std::cout << "[PARAM-COUNT/TX] query($1,$2,$3 -> 3 args): "
//                 << (r.ok ? "OK" : r.error) << "\n";
//     }
//
//     // --- tx.query: 0 placeholders, 0 args ---
//     {
//         auto r = co_await tx.query(
//             "SELECT count(1) FROM param_test_tx");
//         std::cout << "[PARAM-COUNT/TX] query(0 placeholders, 0 args): "
//                 << (r.ok ? "OK" : r.error) << "\n";
//     }
//
//     // --- tx.query_reflect (Obj): struct with 3 fields -> 3 placeholders ---
//     {
//         InsRow row{"bob", 20, "from tx.query_reflect"};
//         auto r = co_await tx.query_reflect(
//             "INSERT INTO param_test_tx(name, value, comment) VALUES($1, $2, $3)",
//             row);
//         std::cout << "[PARAM-COUNT/TX] query_reflect(struct -> $1,$2,$3): "
//                 << (r.ok ? "OK" : r.error) << "\n";
//     }
//
//     // --- tx.exec_reflect (Obj): struct with 3 fields -> 3 placeholders ---
//     {
//         InsRow row{"carol", 30, "from tx.exec_reflect"};
//         auto r = co_await tx.exec_reflect(
//             "INSERT INTO param_test_tx(name, value, comment) VALUES($1, $2, $3)",
//             row);
//         std::cout << "[PARAM-COUNT/TX] exec_reflect(struct -> $1,$2,$3): "
//                 << (r.ok ? "OK" : r.error) << "\n";
//     }
//
//     // --- tx.query_reflect_expected: 2 placeholders, 2 args ---
//     {
//         auto rows = co_await tx.query_reflect_expected<ParamRow>(
//             "SELECT id, name FROM param_test_tx ORDER BY id LIMIT $1 OFFSET $2",
//             10, 0);
//         std::cout << "[PARAM-COUNT/TX] query_reflect_expected($1,$2 -> 2 args): "
//                 << (rows ? "OK n=" + std::to_string(rows->size()) : "FAIL") << "\n";
//     }
//
//     // --- tx.query_reflect_expected_one: 1 placeholder, 1 arg ---
//     {
//         auto one = co_await tx.query_reflect_expected_one<ParamRow>(
//             "SELECT id, name FROM param_test_tx WHERE name = $1 LIMIT 1",
//             std::string("alice"));
//         std::cout << "[PARAM-COUNT/TX] query_reflect_expected_one($1 -> 1 arg): "
//                 << (one ? "OK" : "not found") << "\n";
//     }
//
//     // --- tx.query_reflect_expected (0 args) ---
//     {
//         auto rows = co_await tx.query_reflect_expected<ParamRow>(
//             "SELECT id, name FROM param_test_tx ORDER BY id");
//         std::cout << "[PARAM-COUNT/TX] query_reflect_expected(0 args): "
//                 << (rows ? "OK n=" + std::to_string(rows->size()) : "FAIL") << "\n";
//     }
//
//     // --- tx.query_reflect_expected_one (0 args) ---
//     {
//         auto one = co_await tx.query_reflect_expected_one<ParamRow>(
//             "SELECT id, name FROM param_test_tx ORDER BY id LIMIT 1");
//         std::cout << "[PARAM-COUNT/TX] query_reflect_expected_one(0 args): "
//                 << (one ? "OK" : "not found") << "\n";
//     }
//
//     // --- tx.query_reflect_expected<T, Obj>: struct with 2 fields -> 2 placeholders ---
//     {
//         Filter f{0, 100};
//         auto rows = co_await tx.query_reflect_expected<ParamRow, Filter>(
//             "SELECT id, name FROM param_test_tx WHERE value >= $1 AND value <= $2 ORDER BY id",
//             f);
//         std::cout << "[PARAM-COUNT/TX] query_reflect_expected<T,Obj>(struct -> $1,$2): "
//                 << (rows ? "OK n=" + std::to_string(rows->size()) : "FAIL") << "\n";
//     }
//
//     // --- tx.query_reflect_expected_one<T, Obj>: struct with 1 field -> 1 placeholder ---
//     {
//         ById f{1};
//         auto one = co_await tx.query_reflect_expected_one<ParamRow, ById>(
//             "SELECT id, name FROM param_test_tx WHERE id = $1",
//             f);
//         std::cout << "[PARAM-COUNT/TX] query_reflect_expected_one<T,Obj>(struct -> $1): "
//                 << (one ? "OK" : "not found") << "\n";
//     }
//
//     // --- subtransaction tests ---
//     {
//         auto sub = tx.make_subtx();
//         if (co_await sub.begin()) {
//             // sub.query: 2 placeholders, 2 args
//             {
//                 auto r = co_await sub.query(
//                     "UPDATE param_test_tx SET comment = $1 WHERE id = $2",
//                     "subtx-updated", int64_t{1});
//                 std::cout << "[PARAM-COUNT/SUBTX] query($1,$2 -> 2 args): "
//                         << (r.ok ? "OK" : r.error) << "\n";
//             }
//
//             // sub.query_reflect (Obj): struct with 2 fields -> 2 placeholders
//             {
//                 Upd u{.name = "alice-subtx", .id = 1};
//                 auto r = co_await sub.query_reflect(
//                     "UPDATE param_test_tx SET name = $1 WHERE id = $2", u);
//                 std::cout << "[PARAM-COUNT/SUBTX] query_reflect(struct -> $1,$2): "
//                         << (r.ok ? "OK" : r.error) << "\n";
//             }
//
//             // sub.query_reflect_expected_one: 1 placeholder, 1 arg
//             {
//                 auto one = co_await sub.query_reflect_expected_one<ParamRow>(
//                     "SELECT id, name FROM param_test_tx WHERE id = $1",
//                     int64_t{1});
//                 std::cout << "[PARAM-COUNT/SUBTX] query_reflect_expected_one($1 -> 1 arg): "
//                         << (one ? "OK name=" + one->name : "not found") << "\n";
//             }
//
//             // sub.query_reflect_expected: 1 placeholder, 1 arg
//             {
//                 auto rows = co_await sub.query_reflect_expected<ParamRow>(
//                     "SELECT id, name FROM param_test_tx WHERE value > $1 ORDER BY id",
//                     0);
//                 std::cout << "[PARAM-COUNT/SUBTX] query_reflect_expected($1 -> 1 arg): "
//                         << (rows ? "OK n=" + std::to_string(rows->size()) : "FAIL") << "\n";
//             }
//
//             co_await sub.rollback();
//         }
//     }
//
//     if (!(co_await tx.commit()))
//         std::cout << "[PARAM-COUNT/TX] commit failed\n";
//
//     std::cout << "[PARAM-COUNT/TX] all passed\n";
//     co_return;
// }
//
// task::Awaitable<void> test_param_count_mismatch(usub::pg::PgPool &pool) {
//     // This should trigger assert: 2 placeholders but 3 args
//     auto r = co_await pool.query_awaitable(
//         "SELECT * FROM param_test WHERE id = $1 AND name = $2",
//         1, "alice", "extra_arg");
//     co_return;
// }
//
// task::Awaitable<void> test_param_count_mismatch_tx(usub::pg::PgPool &pool) {
//     usub::pg::PgTransaction tx(&pool);
//     co_await tx.begin();
//     // This should trigger assert: 1 placeholder but 2 args
//     auto r = co_await tx.query(
//         "SELECT * FROM param_test WHERE id = $1",
//         1, "extra_arg");
//     co_await tx.rollback();
//     co_return;
// }

task::Awaitable<void> test_db_query(usub::pg::PgPool &pool) {
    {
        auto res_schema = co_await pool.query_awaitable(
            "CREATE TABLE IF NOT EXISTS public.users("
            "id SERIAL PRIMARY KEY,"
            "name TEXT,"
            "password TEXT"
            ");"
        );

        if (!res_schema.ok) {
            std::cout << "[ERROR] SCHEMA INIT failed: " << res_schema.error << std::endl;
            co_return;
        }
    }
    {
        std::optional<std::string> password = std::nullopt;
        auto res_schema = co_await pool.query_awaitable(
            R"(INSERT INTO users (name, password) VALUES ($1, $2);)",
            "Ivan",
            password
        );

        if (!res_schema.ok) {
            std::cout << "[ERROR] INSERT failed: " << res_schema.error << std::endl;
            co_return;
        }
    }
    {
        auto res = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS array_test (
                id         bigserial PRIMARY KEY,
                test_array text[] NOT NULL,
                comment    text
            );
        )SQL");
        if (!res.ok) {
            std::cout << "[ERROR] CREATE array_test: " << res.error << "\n";
            co_return;
        }

        std::vector<std::string> array = {"test", "array"};
        auto ins = co_await pool.query_awaitable(
            R"(INSERT INTO array_test (test_array, comment) VALUES ($1, $2);)",
            array,
            "comment"
        );

        if (!ins.ok) {
            std::cout << "[ERROR] INSERT array_test: " << ins.error << std::endl;
            co_return;
        }
    }
    {
        usub::pg::PgTransaction txn(&pool);

        if (auto err_begin = co_await txn.begin_errored(); err_begin) {
            co_await txn.finish();
            const auto &e = err_begin.value();

            std::cout << "[TX] begin failed" << toString(e.code) << ", " << e.error << ", " << e.err_detail.sqlstate <<
                    ", "
                    << e.err_detail.message;
            co_return;
        }
        {
            auto r_upd = co_await txn.query(
                "UPDATE users SET name = $1 WHERE id = $2 RETURNING name;",
                "John",
                1
            );

            if (!r_upd.ok) {
                std::cout << "[ERROR] UPDATE failed: " << r_upd.error << std::endl;
                co_await txn.finish();
                co_return;
            }

            if (!r_upd.rows.empty() && !r_upd.rows[0].cols.empty()) {
                std::cout << "[INFO] UPDATE ok, new name=" << r_upd.rows[0].cols[0]
                        << ", affected rows: " << r_upd.rows_affected << std::endl;
            } else {
                std::cout << "[INFO] UPDATE ok, but no RETURNING rows" << std::endl;
            }
        }

        bool ok_commit = co_await txn.commit();
        if (!ok_commit) {
            std::cout << "[ERROR] COMMIT failed" << std::endl;
            co_return;
        }
    }
    {
        auto res_sel = co_await pool.query_awaitable(
            "SELECT id, name FROM users ORDER BY id LIMIT $1;",
            5
        );

        if (!res_sel.ok) {
            std::cout << "[ERROR] SELECT failed: " << res_sel.error << std::endl;
            co_return;
        }

        if (res_sel.rows.empty()) {
            std::cout << "[INFO] SELECT returned no rows" << std::endl;
            co_return;
        }

        std::cout << "[INFO] SELECT results:" << std::endl;
        for (auto &row: res_sel.rows) {
            if (row.cols.size() >= 2)
                std::cout << "  id=" << row.cols[0] << ", name=" << row.cols[1] << std::endl;
            else
                std::cout << "  incomplete row" << std::endl;
        }
    }

    co_return;
}

task::Awaitable<void> test_reflect_query(usub::pg::PgPool &pool) {
    try {
        {
            auto r = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS public.users_reflect (
                id       BIGSERIAL PRIMARY KEY,
                name     TEXT NOT NULL,
                password TEXT,
                roles    INT4[] NOT NULL,
                tags     TEXT[] NOT NULL
            );
        )SQL");
            if (!r.ok) {
                std::cout << "[ERROR] schema: " << r.error << "\n";
                co_return;
            }
        }
        {
            auto r = co_await pool.query_awaitable("TRUNCATE TABLE public.users_reflect RESTART IDENTITY");
            if (!r.ok) {
                std::cout << "[ERROR] truncate: " << r.error << "\n";
                co_return;
            }
        }
        {
            NewUser u{};
            u.name = "Alice";
            u.password = std::nullopt;
            u.roles = {1, 2, 5};
            u.tags = {"admin", "core"};

            auto r = co_await pool.exec_reflect(
                "INSERT INTO users_reflect(name, password, roles, tags) VALUES($1,$2,$3,$4);",
                u
            );
            if (!r.ok) {
                std::cout << "[ERROR] insert: " << r.error << "\n";
                co_return;
            }
            std::cout << "[OK] inserted rows: " << r.rows_affected << "\n";
        }
        {
            std::string name = "Bob";
            std::optional<std::string> pass = std::make_optional<std::string>("x");
            std::vector<int> roles = {3, 4};
            std::vector<std::string> tags = {"beta", "labs"};

            auto r2 = co_await pool.exec_reflect(
                "INSERT INTO users_reflect(name, password, roles, tags) VALUES($1,$2,$3,$4);",
                std::tuple{name, pass, roles, tags}
            );
            if (!r2.ok) {
                std::cout << "[ERROR] insert tuple: " << r2.error << "\n";
                co_return;
            }
            std::cout << "[OK] inserted rows (tuple): " << r2.rows_affected << "\n";
        }
        {
            auto rows = co_await pool.query_reflect_expected<UserRow>(
                "SELECT id, password, name AS username, roles, tags FROM users_reflect ORDER BY id;"
            );
            if (!rows.has_value()) {
                auto &error = rows.error();
                std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                        error.err_detail.message << "\n";
                co_return;
            }

            auto rows_unwrapped = rows.value();

            if (rows_unwrapped.empty()) {
                std::cout << "[INFO] no rows\n";
            } else {
                std::cout << "[INFO] read " << rows_unwrapped.size() << " rows\n";
                for (auto &r: rows_unwrapped) {
                    std::cout << "  id=" << r.id
                            << " name=" << r.username
                            << " password=" << (r.password ? *r.password : "<NULL>")
                            << " roles=[";
                    for (size_t i = 0; i < r.roles.size(); ++i) {
                        if (i) std::cout << ",";
                        std::cout << r.roles[i];
                    }
                    std::cout << "] tags=[";
                    for (size_t i = 0; i < r.tags.size(); ++i) {
                        if (i) std::cout << ",";
                        std::cout << r.tags[i];
                    }
                    std::cout << "]\n";
                }
            }
        }
        {
            auto one = co_await pool.query_reflect_expected_one<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect WHERE name='Alice' LIMIT 1;"
            );

            if (one) std::cout << "[ONE] id=" << one->id << " name=" << one->username << "\n";
            else std::cout << "[ONE] not found\n";
        }
        {
            int64_t qid = 1;
            auto one = co_await pool.query_reflect_expected_one<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect WHERE id = $1",
                qid
            );
            std::cout << "[BY-ID] " << (one ? one->username : "<none>") << "\n";
        }
        {
            std::string q_name = "Alice";
            auto one = co_await pool.query_reflect_expected_one<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect WHERE name = $1",
                q_name
            );
            std::cout << "[BY-NAME] " << (one ? one->username : "<none>") << "\n";
        }
        {
            std::vector<std::string> need_tags{"admin", "labs"};
            auto rows = co_await pool.query_reflect_expected<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect "
                "WHERE tags && $1::text[] "
                "ORDER BY id",
                need_tags
            );

            if (!rows.has_value()) {
                auto &error = rows.error();
                std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                        error.err_detail.message << "\n";
                co_return;
            }

            auto rows_unwrapped = rows.value();
            std::cout << "[TAGS-OVERLAP] n=" << rows_unwrapped.size() << "\n";
        }
        {
            std::array<int, 3> role_set{1, 2, 5};
            auto rows = co_await pool.query_reflect_expected<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect "
                "WHERE roles && $1::int4[] "
                "ORDER BY id",
                role_set
            );

            if (!rows.has_value()) {
                auto &error = rows.error();
                std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                        error.err_detail.message << "\n";
                co_return;
            }

            auto rows_unwrapped = rows.value();

            std::cout << "[ROLES-OVERLAP] n=" << rows_unwrapped.size() << "\n";
        }
        {
            std::optional<std::string> pass = std::nullopt;
            auto rows = co_await pool.query_reflect_expected<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect "
                "WHERE password IS NOT DISTINCT FROM $1 "
                "ORDER BY id",
                pass
            );

            if (!rows.has_value()) {
                auto &error = rows.error();
                std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                        error.err_detail.message << "\n";
                co_return;
            }

            auto rows_unwrapped = rows.value();

            std::cout << "[PWD=NULL] n=" << rows_unwrapped.size() << "\n";
        }
        {
            std::vector<int64_t> ids{1, 2, 3, 4};
            auto rows = co_await pool.query_reflect_expected<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect "
                "WHERE id = ANY($1::int8[]) "
                "ORDER BY id",
                ids
            );

            if (!rows.has_value()) {
                auto &error = rows.error();
                std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                        error.err_detail.message << "\n";
                co_return;
            }

            auto rows_unwrapped = rows.value();

            std::cout << "[ANY(ids)] n=" << rows_unwrapped.size() << "\n";
        }
        {
            int limit = 2, offset = 0;
            auto page = co_await pool.query_reflect_expected<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect "
                "ORDER BY id LIMIT $1 OFFSET $2",
                limit, offset
            );

            if (!page.has_value()) {
                auto &error = page.error();
                std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                        error.err_detail.message << "\n";
                co_return;
            }

            auto page_unwrapped = page.value();

            std::cout << "[PAGE] n=" << page_unwrapped.size()
                    << " (limit=" << limit << ", off=" << offset << ")\n";
        }
        {
            Upd u{.name = "Alice-upd", .id = 1};

            auto ret = co_await pool.query_reflect_expected_one<Ret>(
                "WITH upd AS ("
                "  UPDATE users_reflect SET name = $1 WHERE id = $2 "
                "  RETURNING id, name"
                ") "
                "SELECT id, name AS username FROM upd",
                u
            );

            if (ret) {
                auto check = co_await pool.query_reflect_expected_one<Ret>(
                    "SELECT id, name AS username FROM users_reflect WHERE id = $1",
                    u.id
                );
                std::cout << "[UPDATE->RET] " << (check ? check->username : "<none>") << "\n";
            } else {
                std::cout << "[UPDATE->RET] " << ret->username << "\n";
            }
        }
        {
            std::optional<std::string> patt = std::string("%ali%");
            std::optional<int64_t> min_id = int64_t{0};
            auto rows = co_await pool.query_reflect_expected<UserRow>(
                "SELECT id, name AS username, password, roles, tags "
                "FROM users_reflect "
                "WHERE ($1::text IS NULL OR name ILIKE $1) "
                "AND ($2::int8 IS NULL OR id >= $2) "
                "ORDER BY id",
                patt, min_id
            );
            if (!rows.has_value()) {
                auto &error = rows.error();
                std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                        error.err_detail.message << "\n";
                co_return;
            }

            auto rows_unwrapped = rows.value();

            std::cout << "[MIXED] n=" << rows_unwrapped.size() << "\n";
        }
    } catch (std::exception &e) {
        std::cout << "[EXCEPTION] exception: " << e.what() << "\n";
    }

    co_return;
}

usub::uvent::task::Awaitable<void> tx_reflect_example(usub::pg::PgPool &pool) {
    {
        auto r = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS users_r (
                id       BIGSERIAL PRIMARY KEY,
                name     TEXT        NOT NULL,
                password TEXT,
                roles    INT4[]      NOT NULL DEFAULT '{}',
                tags     TEXT[]      NOT NULL DEFAULT '{}'
            );
        )SQL");
        if (!r.ok) {
            std::cout << "[SCHEMA] " << r.error << "\n";
            co_return;
        }
    }

    usub::pg::PgTransaction tx(&pool);
    if (auto err_begin = co_await tx.begin_errored(); err_begin) {
        co_await tx.finish();
        const auto &e = err_begin.value();

        std::cout << "[TX] begin failed" << toString(e.code) << ", " << e.error << ", " << e.err_detail.sqlstate << ", "
                << e.err_detail.message;
        co_return;
    }
    {
        const char *timeout = "2s";
        auto setr = co_await tx.query("SELECT set_config('lock_timeout', $1, true);", timeout);
        if (!setr.ok) std::cout << "[SET LOCAL] " << setr.error << "\n";
    }

    int64_t inserted_id_1 = 0;
    {
        NewUser nu;
        nu.name = "Kirill";
        nu.password = std::nullopt;
        nu.roles = std::vector<int>{1, 2, 5};
        nu.tags = std::vector<std::string>{"cpp", "uvent", "reflect"};

        auto ins = co_await tx.query_reflect(
            "INSERT INTO users_r(name,password,roles,tags) VALUES($1,$2,$3,$4)", nu);
        if (!ins.ok) {
            std::cout << "[INSERT] " << ins.error << "\n";
            co_await tx.rollback();
            co_return;
        }

        auto ins_ret = co_await tx.query_reflect_expected_one<Ret>(
            "WITH ins AS (INSERT INTO users_r(name,password,roles,tags)"
            " VALUES($1,$2,$3,$4) RETURNING id, name) "
            "SELECT id, name AS username FROM ins",
            nu);

        if (ins_ret) {
            inserted_id_1 = ins_ret->id;
            std::cout << "[INSERT->RET] id=" << ins_ret->id
                    << " user=" << ins_ret->username << "\n";
        } else {
            const auto &e = ins_ret.error();
            std::cout << "[INSERT->RET] fail " << usub::pg::toString(e.code)
                    << " msg=" << e.error << "\n";
        }
    }

    int64_t inserted_id_2 = 0;
    {
        std::string name2 = "Bob";
        std::optional<std::string> pass2 = std::optional<std::string>("x");
        std::vector<int> roles2{3, 4};
        std::vector<std::string> tags2{"beta", "labs"};
        auto tup2 = std::make_tuple(name2, pass2, roles2, tags2);

        auto ret = co_await tx.query_reflect_expected_one<Ret>(
            "WITH ins AS (INSERT INTO users_r(name,password,roles,tags)"
            " VALUES($1,$2,$3,$4) RETURNING id, name) "
            "SELECT id, name AS username FROM ins",
            tup2);

        if (ret) {
            inserted_id_2 = ret->id;
            std::cout << "[INSERT tuple->RET] id=" << ret->id
                    << " user=" << ret->username << "\n";
        } else {
            const auto &e = ret.error();
            std::cout << "[INSERT tuple->RET] fail "
                    << usub::pg::toString(e.code)
                    << " msg=" << e.error << "\n";
        }
    }
    {
        auto sub = tx.make_subtx();
        if (co_await sub.begin()) {
            UpdRoles u{.roles = {9, 9, 9}, .id = inserted_id_1 > 0 ? inserted_id_1 : 1};

            auto r = co_await sub.query_reflect(
                "UPDATE users_r SET roles = $1 WHERE id = $2", u);
            std::cout << "[SUBTX UPDATE] ok=" << r.ok
                    << " affected=" << r.rows_affected << " (rollback)\n";
            co_await sub.rollback();
        }
    }
    {
        auto sub = tx.make_subtx();
        if (co_await sub.begin()) {
            std::vector<std::string> tags_commit{"committed", "subtx"};
            int64_t id2 = inserted_id_2 > 0 ? inserted_id_2 : 2;

            auto r = co_await sub.query(
                "UPDATE users_r SET tags = $1 WHERE id = $2 RETURNING id",
                tags_commit, id2);
            bool ok = r.ok;
            uint64_t aff = r.rows_affected;
            bool committed = co_await sub.commit();
            std::cout << "[SUBTX COMMIT] ok=" << ok
                    << " affected=" << aff
                    << " commit=" << committed << "\n";
        }
    }
    {
        int limit = 10, off = 0;
        auto rows = co_await tx.query_reflect_expected<UserRow>(
            "SELECT id, name AS username, password, roles, tags FROM users_r "
            "ORDER BY id LIMIT $1 OFFSET $2",
            limit, off);

        if (!rows) {
            const auto &e = rows.error();
            std::cout << "[SELECT PAGE] fail "
                    << usub::pg::toString(e.code)
                    << " msg=" << e.error << "\n";
        } else {
            std::cout << "[SELECT PAGE] n=" << rows->size() << "\n";
            for (auto &u: *rows) {
                std::cout << "  id=" << u.id << " name=" << u.username
                        << " pwd=" << (u.password ? *u.password : "<NULL>")
                        << " roles=[";
                for (size_t i = 0; i < u.roles.size(); ++i)
                    std::cout << u.roles[i] << (i + 1 < u.roles.size() ? "," : "");
                std::cout << "] tags=[";
                for (size_t i = 0; i < u.tags.size(); ++i)
                    std::cout << u.tags[i] << (i + 1 < u.tags.size() ? "," : "");
                std::cout << "]\n";
            }
        }
    }
    {
        std::optional<std::string> patt = std::string("%bo%");
        std::optional<int64_t> min_id = int64_t{0};
        auto rows = co_await tx.query_reflect_expected<UserRow>(
            "SELECT id, name AS username, password, roles, tags FROM users_r "
            "WHERE ($1::text IS NULL OR name ILIKE $1) "
            "AND ($2::int8 IS NULL OR id >= $2) "
            "ORDER BY id",
            patt, min_id);

        if (!rows) {
            const auto &e = rows.error();
            std::cout << "[FILTERED] fail "
                    << usub::pg::toString(e.code)
                    << " msg=" << e.error << "\n";
        } else {
            std::cout << "[FILTERED] n=" << rows->size() << "\n";
        }
    }
    {
        Upd u{
            .name = "Kirill-upd",
            .id = inserted_id_1 > 0 ? inserted_id_1 : 1
        };

        auto upd = co_await tx.query_reflect(
            "UPDATE users_r SET name = $1 WHERE id = $2", u);
        std::cout << "[TX UPDATE] ok=" << upd.ok
                << " affected=" << upd.rows_affected << "\n";

        auto check = co_await tx.query_reflect_expected_one<UserRow>(
            "SELECT id, name AS username, password, roles, tags FROM users_r WHERE id = $1",
            u.id);

        std::cout << "[TX CHECK] name="
                << (check ? check->username : std::string("<none>")) << "\n";
    }
    {
        std::vector<int64_t> ids;
        if (inserted_id_1 > 0) ids.push_back(inserted_id_1);
        if (inserted_id_2 > 0) ids.push_back(inserted_id_2);

        auto rows = co_await tx.query_reflect_expected<UserRow>(
            "SELECT id, name AS username, password, roles, tags "
            "FROM users_r WHERE id = ANY($1::int8[]) ORDER BY id",
            ids);

        if (!rows) {
            const auto &e = rows.error();
            std::cout << "[ANY(ids)] fail "
                    << usub::pg::toString(e.code)
                    << " msg=" << e.error << "\n";
        } else {
            std::cout << "[ANY(ids)] n=" << rows->size() << "\n";
        }
    }

    if (!(co_await tx.commit())) std::cout << "[TX] commit failed\n";
    co_return;
}

task::Awaitable<void> test_array_inserts(usub::pg::PgPool &pool) {
    {
        auto res = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS array_test (
                id         bigserial PRIMARY KEY,
                test_array text[] NOT NULL,
                comment    text
            );
        )SQL");
        if (!res.ok) {
            std::cout << "[ERROR] CREATE array_test: " << res.error << "\n";
            co_return;
        }

        std::vector<std::string> array = {"test", "array"};
        auto ins = co_await pool.query_awaitable(
            R"(INSERT INTO array_test (test_array, comment) VALUES ($1, $2);)",
            array,
            "comment"
        );
        if (!ins.ok) {
            std::cout << "[ERROR] INSERT array_test: " << ins.error << "\n";
            co_return;
        }
    }
    {
        auto res = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS array_test_multi (
                id        bigserial PRIMARY KEY,
                a_int4_1  int4[]   NOT NULL,
                a_int4_2  int4[]   NOT NULL,
                a_float8  float8[] NOT NULL,
                a_bool    bool[]   NOT NULL,
                a_text    text[]   NOT NULL,
                comment   text
            );
        )SQL");
        if (!res.ok) {
            std::cout << "[ERROR] CREATE array_test_multi: " << res.error << "\n";
            co_return;
        }

        std::array<int, 3> ai{1, 2, 3};
        int ci[3]{4, 5, 6};
        std::list<double> ld{1.25, 2.5};
        std::vector<std::optional<bool> > vb{true, std::nullopt, false};
        std::initializer_list<const char *> il = {"x", "y"};

        auto ins = co_await pool.query_awaitable(
            R"(INSERT INTO array_test_multi
               (a_int4_1, a_int4_2, a_float8, a_bool, a_text, comment)
               VALUES ($1, $2, $3, $4, $5, $6);)",
            ai,
            ci,
            ld,
            vb,
            il,
            "multi-insert"
        );
        if (!ins.ok) {
            std::cout << "[ERROR] INSERT array_test_multi: " << ins.error << "\n";
            co_return;
        }
    }
    {
        auto q1 = co_await pool.query_awaitable("SELECT count(1) FROM array_test;");
        if (!q1.ok) {
            std::cout << "[ERROR] SELECT array_test: " << q1.error << "\n";
            co_return;
        }
        auto q2 = co_await pool.query_awaitable("SELECT count(1) FROM array_test_multi;");
        if (!q2.ok) {
            std::cout << "[ERROR] SELECT array_test_multi: " << q2.error << "\n";
            co_return;
        }

        std::cout << "array_test rows=" << (q1.rows.empty() ? "?" : q1.rows[0].cols[0])
                << ", array_test_multi rows=" << (q2.rows.empty() ? "?" : q2.rows[0].cols[0]) << "\n";
    }

    co_return;
}

struct MyNotifyHandler {
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel,
               std::string payload,
               int backend_pid) {
        std::cout << "[NOTIFY] ch=" << channel
                << " pid=" << backend_pid
                << " payload=" << payload << std::endl;

        auto res = co_await this->pool->query_awaitable(
            "SELECT id, name FROM users WHERE id = $1;",
            1
        );

        if (res.ok && !res.rows.empty()) {
            std::cout << "reactive fetch -> id=" << res.rows[0].cols[0]
                    << ", name=" << res.rows[0].cols[1] << std::endl;
        } else {
            std::cout << "reactive fetch fail: " << res.error << std::endl;
        }

        co_return;
    }

    usub::pg::PgPool *pool;
};

task::Awaitable<void> spawn_listener(usub::pg::PgPool &pool) {
    auto c = co_await pool.acquire_connection();
    if (!c || !*c || !(*c)->connected()) {
        co_return;
    }

    using Listener = usub::pg::PgNotificationListener<MyNotifyHandler>;
    auto listener = std::make_shared<Listener>(
        "events",
        *c
    );

    listener->setHandler(MyNotifyHandler{&pool});

    co_await listener->run();
    co_return;
}

struct BalanceLogger : usub::pg::IPgNotifyHandler {
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel,
               std::string payload,
               int backend_pid) override {
        std::cout << "[BALANCE] pid=" << backend_pid
                << " payload=" << payload << "\n";
        co_return;
    }
};

struct RiskAlerter : usub::pg::IPgNotifyHandler {
    usub::uvent::task::Awaitable<void>
    operator()(std::string channel,
               std::string payload,
               int backend_pid) override {
        std::cout << "[RISK] pid=" << backend_pid
                << " payload=" << payload << "\n";
        co_return;
    }
};

usub::uvent::task::Awaitable<void> spawn_listener_multiplexer(usub::pg::PgPool &pool) {
    auto c = co_await pool.acquire_connection();
    if (!c || !*c || !(*c)->connected()) {
        std::cout << "Failed to get connection for multiplexer\n";
        co_return;
    }

    usub::pg::PgNotificationMultiplexer mux(
        *c,
        "localhost",
        "12432",
        "postgres",
        "postgres",
        "password",
        {512}
    );

    auto h1 = co_await mux.add_handler(
        "balances.updated",
        std::make_shared<BalanceLogger>()
    );

    auto h2 = co_await mux.add_handler(
        "risk.test",
        std::make_shared<RiskAlerter>()
    );

    if (!h1.has_value() || !h2.has_value()) {
        std::cout << "Failed to subscribe one or more channels\n";
        co_return;
    }

    co_await mux.run();
    co_return;
}

usub::uvent::task::Awaitable<void> routing_example() {
    using namespace usub::pg;
    using namespace std::chrono_literals;

    PgConnector router = PgConnectorBuilder{}
            .node("primary1", "localhost", "12432", "postgres", "postgres", "password", NodeRole::Primary,
                  1, 16)
            .node("replica1", "localhost", "12432", "postgres", "postgres", "password",
                  NodeRole::AsyncReplica, 2, 16)
            .primary_failover({"primary1", "replica1"})
            .default_consistency(Consistency::BoundedStaleness)
            .bounded_staleness(150ms, 0)
            .read_my_writes_ttl(500ms)
            .pool_limits(64, 16)
            .health(10000, 120, "SELECT 1")
            .build();

    usub::uvent::system::co_spawn(router.start_health_loop());
    co_await usub::uvent::system::this_coroutine::sleep_for(1500ms);

    RouteHint read_hint{.kind = QueryKind::Read, .consistency = Consistency::Eventual};
    if (auto *pool = router.route(read_hint)) {
        auto res = co_await pool->query_awaitable("SELECT now()");
        std::cout << (res.ok ? "read ok\n" : "read fail\n");
    }

    RouteHint write_hint{.kind = QueryKind::Write, .consistency = Consistency::Strong};
    if (auto *pool = router.route(write_hint)) {
        auto res = co_await pool->query_awaitable("INSERT INTO logs(ts) VALUES (now())");
        std::cout << (res.ok ? "write ok\n" : "write fail\n");
    }

    co_return;
}

struct UserErrorRow {
    int id;
    std::string name;
    double balance;
};

usub::uvent::task::Awaitable<void> decode_fail_example(usub::pg::PgPool &pool) {
    {
        auto r = co_await pool.query_awaitable(R"SQL(
            DROP TABLE IF EXISTS users_r;
            CREATE TABLE users_r (
                id       BIGSERIAL PRIMARY KEY,
                name     TEXT,
                balance  TEXT
            );
        )SQL");
        if (!r.ok) co_return;
    }
    {
        auto ins = co_await pool.query_awaitable(
            "INSERT INTO users_r(name,balance) VALUES('Alice','not_a_number')");
        if (!ins.ok) std::cout << "[INSERT ERROR] " << ins.error << "\n";
    }

    try {
        auto rows = co_await pool.query_reflect_expected<UserErrorRow>(
            "SELECT id, name, balance FROM users_r");

        if (!rows.has_value()) {
            auto &error = rows.error();
            std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                    error.err_detail.message << "\n";
            co_return;
        }

        auto rows_unwrapped = rows.value();

        std::cout << "[ROWS] n=" << rows_unwrapped.size() << "\n";
    } catch (const std::exception &ex) {
        std::cerr << "!!! Decode error caught: " << ex.what() << "\n";
    }

    co_return;
}

usub::uvent::task::Awaitable<void> expected_reflect_example(usub::pg::PgPool &pool) {
    using usub::pg::PgErrorCode;
    using usub::pg::toString;
    {
        auto r = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS users_exp (
                id       BIGSERIAL PRIMARY KEY,
                name     TEXT NOT NULL,
                password TEXT,
                roles    INT4[] NOT NULL DEFAULT '{}',
                tags     TEXT[] NOT NULL DEFAULT '{}'
            );
        )SQL");
        if (!r.ok) {
            std::cout << "[EXP/SCHEMA] " << r.error << "\n";
            co_return;
        }

        auto t1 = co_await pool.query_awaitable("TRUNCATE users_exp RESTART IDENTITY");
        if (!t1.ok) {
            std::cout << "[EXP/TRUNCATE] " << t1.error << "\n";
            co_return;
        }

        NewUser a{.name = "Alice", .password = std::nullopt, .roles = {1, 2}, .tags = {"alpha"}};
        NewUser b{.name = "Bob", .password = std::string("x"), .roles = {3}, .tags = {"beta", "labs"}};

        auto i1 = co_await pool.exec_reflect(
            "INSERT INTO users_exp(name,password,roles,tags) VALUES($1,$2,$3,$4)", a);
        auto i2 = co_await pool.exec_reflect(
            "INSERT INTO users_exp(name,password,roles,tags) VALUES($1,$2,$3,$4)", b);
        if (!i1.ok || !i2.ok) {
            std::cout << "[EXP/INSERT] " << (i1.ok ? "" : i1.error) << " " << (i2.ok ? "" : i2.error) << "\n";
            co_return;
        }
    }
    {
        auto exp_rows = co_await pool.query_reflect_expected<UserRow>(
            "SELECT id, name AS username, password, roles, tags FROM users_exp ORDER BY id");

        if (!exp_rows) {
            const auto &e = exp_rows.error();
            std::cout << "[EXP/SELECT] fail code=" << toString(e.code)
                    << " sqlstate=" << e.err_detail.sqlstate
                    << " msg=" << e.error << "\n";
        } else {
            std::cout << "[EXP/SELECT] n=" << exp_rows->size() << "\n";
        }
    }
    {
        auto one = co_await pool.query_reflect_expected_one<UserRow>(
            "SELECT id, name AS username, password, roles, tags FROM users_exp WHERE name = $1 LIMIT 1",
            std::string("Alice"));

        if (!one) {
            const auto &e = one.error();
            std::cout << "[EXP/ONE Alice] fail code=" << toString(e.code)
                    << " msg=" << e.error << "\n";
        } else {
            std::cout << "[EXP/ONE Alice] id=" << one->id << " user=" << one->username << "\n";
        }
    }
    {
        auto one = co_await pool.query_reflect_expected_one<UserRow>(
            "SELECT id, name AS username, password, roles, tags FROM users_exp WHERE name = $1 LIMIT 1",
            std::string("Nobody"));

        if (!one) {
            const auto &e = one.error();
            std::cout << "[EXP/ONE Nobody] fail code=" << toString(e.code)
                    << " msg=" << e.error << "\n";
        } else {
            std::cout << "[EXP/ONE Nobody] unexpected row id=" << one->id << "\n";
        }
    }
    {
        usub::pg::PgTransaction tx(&pool);
        if (auto err_begin = co_await tx.begin_errored(); err_begin) {
            co_await tx.finish();
            const auto &e = err_begin.value();

            std::cout << "[TX] begin failed" << toString(e.code) << ", " << e.error << ", " << e.err_detail.sqlstate <<
                    ", "
                    << e.err_detail.message;
            co_return;
        }

        Upd u{.name = "Alice_exp_upd", .id = 1};
        auto upd = co_await tx.query_reflect_expected_one<Ret>(
            "UPDATE users_exp SET name = $1 WHERE id = $2 RETURNING id, name AS username",
            u);

        if (!upd) {
            const auto &e = upd.error();
            std::cout << "[EXP/TX/UPDATE] fail code=" << toString(e.code)
                    << " sqlstate=" << e.err_detail.sqlstate
                    << " msg=" << e.error << "\n";
            co_await tx.rollback();
            co_return;
        } else {
            std::cout << "[EXP/TX/UPDATE] id=" << upd->id << " user=" << upd->username << "\n";
        }

        auto list = co_await tx.query_reflect_expected<UserRow>(
            "SELECT id, name AS username, password, roles, tags FROM users_exp ORDER BY id");
        if (!list) {
            const auto &e = list.error();
            std::cout << "[EXP/TX/SELECT] fail code=" << toString(e.code)
                    << " msg=" << e.error << "\n";
            co_await tx.rollback();
            co_return;
        }
        std::cout << "[EXP/TX/SELECT] n=" << list->size() << "\n";

        if (!(co_await tx.commit())) std::cout << "[EXP/TX] commit failed\n";
    }
    {
        auto bad = co_await pool.query_reflect_expected<UserRow>(
            "SELECT id, non_existing AS username, password, roles, tags FROM users_exp");
        if (!bad) {
            const auto &e = bad.error();
            std::cout << "[EXP/ERROR demo] code=" << toString(e.code)
                    << " sqlstate=" << e.err_detail.sqlstate
                    << " category=" << usub::pg::toString(e.err_detail.category)
                    << " msg=" << e.error << "\n";
        }
    }

    co_return;
}

enum class RoleKind { admin, user, guest };

enum class RoleKind2 { admin1, user1, guest1 };

template<>
struct usub::pg::detail::upq::enum_meta<RoleKind> {
    static constexpr auto mapping = enumerate<
        RoleKind::admin,
        RoleKind::user,
        RoleKind::guest
    >();
};

template<>
struct usub::pg::detail::upq::enum_meta<RoleKind2> {
    static constexpr auto mapping = enumerate<
        RoleKind2::admin1,
        RoleKind2::user1,
        RoleKind2::guest1
    >();
};

struct EnumIns {
    std::string name;
    RoleKind kind;
    std::optional<RoleKind> alt_kind;
    std::vector<RoleKind> kinds;
};

struct EnumRow {
    int64_t id;
    std::string name;
    RoleKind kind;
    std::optional<RoleKind> alt_kind;
    std::vector<RoleKind> kinds;
};

task::Awaitable<void> test_enum_support(usub::pg::PgPool &pool) {
    {
        auto r = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS users_enum (
                id        BIGSERIAL PRIMARY KEY,
                name      TEXT        NOT NULL,
                kind      TEXT        NOT NULL,
                alt_kind  TEXT        NULL,
                kinds     TEXT[]      NOT NULL DEFAULT '{}'
            );
        )SQL");
        if (!r.ok) {
            std::cout << "[ENUM/SCHEMA] " << r.error << "\n";
            co_return;
        }
        auto t = co_await pool.query_awaitable("TRUNCATE users_enum RESTART IDENTITY");
        if (!t.ok) {
            std::cout << "[ENUM/TRUNCATE] " << t.error << "\n";
            co_return;
        }
    }
    {
        EnumIns u{
            .name = "Alice",
            .kind = RoleKind::admin,
            .alt_kind = std::nullopt,
            .kinds = {RoleKind::admin, RoleKind::user}
        };
        auto ins = co_await pool.exec_reflect(
            "INSERT INTO users_enum(name, kind, alt_kind, kinds) VALUES($1,$2,$3,$4)", u);
        if (!ins.ok) {
            std::cout << "[ENUM/INSERT#1] " << ins.error << "\n";
            co_return;
        }
    }
    {
        std::string name = "Bob";
        RoleKind kind = RoleKind::user;
        std::optional<RoleKind> alt = RoleKind::guest;
        std::vector<RoleKind> kinds{RoleKind::user, RoleKind::guest};
        auto ins2 = co_await pool.exec_reflect(
            "INSERT INTO users_enum(name, kind, alt_kind, kinds) VALUES($1,$2,$3,$4)",
            std::tuple{name, kind, alt, kinds});
        if (!ins2.ok) {
            std::cout << "[ENUM/INSERT#2] " << ins2.error << "\n";
            co_return;
        }
    }
    {
        auto rows = co_await pool.query_reflect_expected<EnumRow>(
            "SELECT id, name, kind, alt_kind, kinds FROM users_enum ORDER BY id");
        if (!rows.has_value()) {
            auto &error = rows.error();
            std::cout << "PgQuery failed: " << toString(error.code) << ", " << error.err_detail.sqlstate << ", " <<
                    error.err_detail.message << "\n";
            co_return;
        }
        auto rows_unwrapped = rows.value();
        std::cout << "[ENUM/SELECT] n=" << rows_unwrapped.size() << "\n";
        for (auto &r: rows_unwrapped) {
            std::cout << "  id=" << r.id
                    << " name=" << r.name
                    << " kind=" << (r.kind == RoleKind::admin ? "admin" : r.kind == RoleKind::user ? "user" : "guest")
                    << " alt=" << (r.alt_kind
                                       ? (*r.alt_kind == RoleKind::admin
                                              ? "admin"
                                              : *r.alt_kind == RoleKind::user
                                                    ? "user"
                                                    : "guest")
                                       : "<NULL>")
                    << " kinds=[";
            for (size_t i = 0; i < r.kinds.size(); ++i) {
                if (i) std::cout << ",";
                auto k = r.kinds[i];
                std::cout << (k == RoleKind::admin ? "admin" : k == RoleKind::user ? "user" : "guest");
            }
            std::cout << "]\n";
        }
    }
    {
        auto rows = co_await pool.query_reflect_expected<EnumRow>(
            "SELECT id, name, kind, alt_kind, kinds FROM users_enum WHERE kind = $1 ORDER BY id",
            RoleKind::user);
        std::cout << "[ENUM/FILTER kind=user] n=" << rows.value().size() << "\n";
    }
    {
        auto rows = co_await pool.query_reflect_expected<EnumRow>(
            "SELECT id, name, kind, alt_kind, kinds FROM users_enum WHERE alt_kind IS NULL ORDER BY id");
        std::cout << "[ENUM/FILTER alt_kind IS NULL] n=" << rows->size() << "\n";
    }
    {
        std::vector<RoleKind> need{RoleKind::admin, RoleKind::guest};
        auto rows = co_await pool.query_reflect_expected<EnumRow>(
            "SELECT id, name, kind, alt_kind, kinds FROM users_enum "
            "WHERE kinds && $1::text[] ORDER BY id",
            need);
        std::cout << "[ENUM/OVERLAP kinds] n=" << rows.value().size() << "\n";
    }

    co_return;
}

struct Profile {
    int age{};
    std::optional<std::string> city;
    std::vector<std::string> flags;
};

struct UserRowStrict {
    int64_t id{};
    std::string username;
    usub::pg::PgJson<Profile> profile; // strict parse
};

struct UserRowLoose {
    int64_t id{};
    std::string username;
    usub::pg::PgJson<Profile, false> profile; // non-strict parse
};

struct UserJsonRowStrict {
    int64_t id{};
    std::string username;
    usub::pg::PgJson<Profile, true> profile; // strict parse
};

struct UserJsonRowLoose {
    int64_t id{};
    std::string username;
    usub::pg::PgJson<Profile, false> profile; // non-strict parse
};

static void print_ujson_err(const ujson::Error &e, std::string_view src) {
    std::cerr << "UJSON ERR: " << (e.msg ? e.msg : "<null>") << "\n"
            << "pos=" << e.pos << " line=" << e.line << " col=" << e.col << "\n"
            << "path=" << e.path << "\n"
            << e.near(src) << "\n";
}

static void print_pg_err(const usub::pg::QueryResult &r) {
    std::cerr << "PG FAIL code=" << usub::pg::toString(r.code)
            << " sqlstate=" << r.err_detail.sqlstate
            << " msg='" << r.error << "'\n";
}

usub::uvent::task::Awaitable<void> test_pgjson_ujson(usub::pg::PgPool &pool) {
    using namespace usub::pg;

    std::cout << "[JSON] start\n";
    {
        auto r = co_await pool.query_awaitable(R"SQL(
            CREATE TABLE IF NOT EXISTS users_json_demo (
                id        BIGSERIAL PRIMARY KEY,
                username  TEXT  NOT NULL,
                profile   JSONB NOT NULL
            );
        )SQL");
        if (!r.ok) {
            std::cerr << "[JSON/SCHEMA] " << r.error << "\n";
            co_return;
        }

        auto t = co_await pool.query_awaitable("TRUNCATE users_json_demo RESTART IDENTITY");
        if (!t.ok) {
            std::cerr << "[JSON/TRUNCATE] " << t.error << "\n";
            co_return;
        }

        std::cout << "[JSON/SCHEMA+TRUNCATE] OK\n";
    }
    {
        Profile p{.age = 27, .city = std::string("AMS"), .flags = {"a", "b"}};
        std::string name = "kirill";

        auto ins = co_await pool.exec_reflect(
            "INSERT INTO users_json_demo(username, profile) VALUES($1,$2)",
            std::tuple{name, usub::pg::pg_jsonb(p)}
        );
        if (!ins.ok) {
            std::cerr << "[JSON/INSERT good] " << ins.error << "\n";
            co_return;
        }
        std::cout << "[JSON/INSERT good] OK\n";
    }
    {
        auto ins = co_await pool.query_awaitable(R"SQL(
            INSERT INTO users_json_demo(username, profile)
            VALUES ('broken', '{"age":1,"city":"A","flags":["x"],"UNKNOWN":123}'::jsonb);
        )SQL");
        if (!ins.ok) {
            std::cerr << "[JSON/INSERT broken] " << ins.error << "\n";
            co_return;
        }
        std::cout << "[JSON/INSERT broken] OK\n";
    }
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
            for (auto &r: *rows) {
                std::cout << "  id=" << r.id
                        << " username=" << r.username
                        << " age=" << r.profile.value.age
                        << " city=" << (r.profile.value.city ? *r.profile.value.city : "<NULL>")
                        << " flags=" << r.profile.value.flags.size()
                        << "\n";
            }
        }
    }
    {
        auto rows = co_await pool.query_reflect_expected<UserJsonRowStrict>(
            "SELECT id, username, profile FROM users_json_demo WHERE username='broken' LIMIT 1"
        );

        if (!rows) {
            std::cout << "[JSON/SELECT strict broken] EXPECTED FAIL code=" << toString(rows.error().code)
                    << " err='" << rows.error().error << "'\n";
        } else {
            std::cout << "[JSON/SELECT strict broken] UNEXPECTED OK n=" << rows->size() << "\n";
        }
    }
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
        for (auto &r: *rows) {
            std::cout << "  id=" << r.id
                    << " username=" << r.username
                    << " age=" << r.profile.value.age
                    << " city=" << (r.profile.value.city ? *r.profile.value.city : "<NULL>")
                    << " flags=" << r.profile.value.flags.size()
                    << "\n";
        }
    }

    co_return;
}

int main() {
    settings::timeout_duration_ms = 5000;

    usub::Uvent uvent(1);

    usub::pg::PgPool pool(
        "localhost",
        "12432",
        "postgres",
        "postgres",
        "password",
        32
    );

    uvent.for_each_thread([&](int threadIndex, thread::ThreadLocalStorage *tls) {
        system::co_spawn_static(
            test_db_query(pool),
            threadIndex
        );
    });

    system::co_spawn(spawn_listener_multiplexer(pool));
    system::co_spawn(spawn_listener(pool));
    system::co_spawn(test_array_inserts(pool));
    system::co_spawn(test_reflect_query(pool));
    system::co_spawn(tx_reflect_example(pool));
    system::co_spawn(routing_example());
    system::co_spawn(decode_fail_example(pool));
    system::co_spawn(expected_reflect_example(pool));
    system::co_spawn(test_enum_support(pool));
    system::co_spawn(test_pgjson_ujson(pool));

    // system::co_spawn(test_param_count_pool(pool));
    // system::co_spawn(test_param_count_transaction(pool));
    //
    // system::co_spawn(test_param_count_mismatch(pool));
    // system::co_spawn(test_param_count_mismatch_tx(pool));

    uvent.run();
    return 0;
}
