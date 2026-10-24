// Functional and failure-path tests for upq.
//
//   UPQ_TEST_PG="host port user password db"   live PostgreSQL (most cases)
//   UPQ_TEST_FAKE_PORT=<port>                   tests/fake_pg_server.py (retry matrix)
//   upq_tests [name-substring]
//
// Built with -DUPQ_TESTS_BASELINE the file compiles against the pre-refactor
// upq too (only the cases that use the old API), to show which of them the
// old code fails.
//
// Every case runs in its own forked child with its own uvent runtime (several
// runtimes in one process share stale thread-locals) and a 30 s watchdog, so a
// hang shows up as a timeout instead of a stuck run. Cases whose server is not
// configured are skipped.

#include <fcntl.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "uvent/Uvent.h"
#include "upq/PgNotificationMultiplexer.h"
#include "upq/PgPool.h"
#ifndef UPQ_TESTS_BASELINE
#include "upq/PgRetry.h"
#endif
#include "upq/PgTransaction.h"

using namespace usub::uvent;
using namespace usub::pg;
using namespace std::chrono_literals;

namespace {
    // ---------- harness ----------

    std::atomic<bool> g_failed{false};

    void report_failure(const char *file, int line, const std::string &what) {
        std::fprintf(stderr, "    FAILED %s:%d: %s\n", file, line, what.c_str());
        g_failed = true;
    }

#define CHECK(cond)                                                     \
    do {                                                                \
        if (!(cond)) {                                                  \
            report_failure(__FILE__, __LINE__, #cond);                  \
            co_return;                                                  \
        }                                                               \
    } while (0)

#define CHECK_MSG(cond, msg)                                            \
    do {                                                                \
        if (!(cond)) {                                                  \
            report_failure(__FILE__, __LINE__, std::string(#cond) + " | " + (msg)); \
            co_return;                                                  \
        }                                                               \
    } while (0)

    struct Server {
        std::string host, port, user, password, db;
    };

    std::optional<Server> g_pg;
    std::optional<std::string> g_fake_port;

    PgPool *pg_pool(size_t size) {
        return new PgPool(g_pg->host, g_pg->port, g_pg->user, g_pg->db, g_pg->password, size, 3,
                          SSLConfig{.mode = SSLMode::disable});
    }

    PgPool *fake_pool(const std::string &db, size_t size = 2) {
        auto *p = new PgPool("127.0.0.1", *g_fake_port, "u", db, "p", size, 3, SSLConfig{.mode = SSLMode::disable});
        p->set_transient_retry(3, 10);  // fast backoff for tests
        return p;
    }

    std::string unique(const char *prefix) {
        static std::atomic<int> n{0};
        return std::string(prefix) + "_" + std::to_string(getpid()) + "_" + std::to_string(n++);
    }

    task::Awaitable<int64_t> backend_pid(PgPool &pool) {
        auto r = co_await pool.query_awaitable("SELECT pg_backend_pid()::bigint");
        co_return r.ok && !r.rows.empty() ? std::stoll(r.rows[0][0]) : -1;
    }

    // ---------- cases: statements ----------

    task::Awaitable<void> t_simple_query() {
        auto &pool = *pg_pool(2);
        auto r = co_await pool.query_awaitable("SELECT 1 AS a, 'x' AS b");
        CHECK_MSG(r.ok, r.error);
        CHECK(r.rows.size() == 1);
        CHECK(r.columns.size() == 2 && r.columns[0] == "a" && r.columns[1] == "b");
        CHECK(r.rows[0][0] == "1" && r.rows[0][1] == "x");
        CHECK(r.rows_affected == 1);
    }

    task::Awaitable<void> t_param_types() {
        auto &pool = *pg_pool(2);
        const std::string s = "it's \"quoted\" \\ back";
        const std::string_view sv = std::string_view(s).substr(0, 4);  // not NUL-terminated
        const char *cs = "c-string";
        std::optional<int32_t> none;
        std::optional<int32_t> some = 7;
        std::vector<int64_t> ints{1, -2, 3};
        std::vector<std::string> strs{"a,b", "c\"d", "e\\f", ""};
        std::vector<std::optional<int32_t> > opt_ints{1, std::nullopt, 3};
        std::vector<double> dbl{0.1 + 0.2, 1e-300};
        auto r = co_await pool.query_awaitable(
            "SELECT $1::int2 = 5, $2::int4 = -6, $3::int8 = 7000000000, $4::bool, $5::float4 = 1.5::float4, "
            "$6::float8 = 2.25, $7::text = 'it''s \"quoted\" \\ back', $8::text = 'it''s', $9::text = 'c-string', "
            "$10::text = 'literal', $11::int4 IS NULL, $12::int4 = 7, $13::int8[] = ARRAY[1,-2,3]::int8[], "
            "$14::text[] = ARRAY['a,b', 'c\"d', 'e\\f', '']::text[], "
            "$15::int4[] IS NOT DISTINCT FROM ARRAY[1,NULL,3]::int4[], "
            "$16::float8[] = ARRAY[0.30000000000000004::float8, 1e-300::float8]",
            int16_t{5}, int32_t{-6}, int64_t{7000000000}, true, 1.5f, 2.25, s, sv, cs, "literal", none, some, ints,
            strs, opt_ints, dbl);
        CHECK_MSG(r.ok, r.error);
        CHECK(r.rows.size() == 1);
        for (size_t i = 0; i < r.rows[0].size(); ++i)
            CHECK_MSG(r.rows[0][i] == "t", "param #" + std::to_string(i + 1) + " = " + r.rows[0][i]);
    }

    struct NamedRow {
        int64_t id;
        std::string name;
        std::optional<std::string> note;
        double score;
    };

    struct OtherNames {
        int64_t x;
        std::string y;
        std::optional<std::string> z;
        double w;
    };

    task::Awaitable<void> t_reflect_named_and_positional() {
        auto &pool = *pg_pool(2);
        // Columns in a different order than the fields: only named mapping gets this right.
        auto named = co_await pool.query_reflect_expected<NamedRow>(
            "SELECT 1.5::float8 AS score, NULL::text AS note, 'n' || g AS name, g::bigint AS id "
            "FROM generate_series(1, 3) g ORDER BY g");
        CHECK(named.has_value());
        CHECK(named->size() == 3);
        CHECK((*named)[2].id == 3 && (*named)[2].name == "n3" && !(*named)[2].note && (*named)[2].score == 1.5);

        // Field names do not match: positional mapping.
        auto pos = co_await pool.query_reflect_expected<OtherNames>(
            "SELECT g::bigint, 'n' || g, 'note', 2.5::float8 FROM generate_series(1, 2) g ORDER BY g");
        CHECK(pos.has_value());
        CHECK(pos->size() == 2 && (*pos)[1].x == 2 && (*pos)[1].y == "n2" && (*pos)[1].z == "note");

        auto one = co_await pool.query_reflect_expected_one<NamedRow>(
            "SELECT $1::bigint AS id, 'a' AS name, 'b' AS note, 0.5::float8 AS score", int64_t{42});
        CHECK(one.has_value() && one->id == 42 && one->note == "b");
    }

    task::Awaitable<void> t_no_rows_and_errors_keep_connection() {
        auto &pool = *pg_pool(1);
        const int64_t pid = co_await backend_pid(pool);
        CHECK(pid > 0);

        auto none = co_await pool.query_reflect_expected_one<NamedRow>(
            "SELECT 1::bigint AS id, 'a' AS name, NULL::text AS note, 1.0::float8 AS score WHERE false");
        CHECK(!none.has_value());
        CHECK(none.error().code == PgErrorCode::Unknown && none.error().error == "no rows");

        auto div = co_await pool.query_reflect_expected<NamedRow>("SELECT 1/0");
        CHECK(!div.has_value());
        CHECK(div.error().code == PgErrorCode::ServerError && div.error().err_detail.sqlstate == "22012");

        auto raw = co_await pool.query_awaitable("SELECT * FROM upq_no_such_table");
        CHECK(!raw.ok && raw.err_detail.sqlstate == "42P01");

        // Same backend: none of the above cost a reconnect.
        CHECK(co_await backend_pid(pool) == pid);
    }

    struct IntOnly {
        int64_t v;
    };

    task::Awaitable<void> t_mapping_exception_releases_connection() {
        auto &pool = *pg_pool(1);
        bool threw = false;
        try {
            auto r = co_await pool.query_reflect_expected<IntOnly>("SELECT 'not a number' AS v");
            (void) r;
        } catch (const std::exception &) {
            threw = true;
        }
        CHECK(threw);
        // Before the refactor the pooled connection leaked here and a pool of
        // one waited forever on the next acquire.
        auto again = co_await pool.query_awaitable("SELECT 1");
        CHECK(again.ok);
    }

    task::Awaitable<void> t_multi_statement() {
        auto &pool = *pg_pool(1);
        auto r = co_await pool.query_awaitable("SELECT 1 AS a; SELECT 2 AS a");
        CHECK_MSG(r.ok, r.error);
        CHECK(r.rows.size() == 2 && r.rows[0][0] == "1" && r.rows[1][0] == "2");

        auto e = co_await pool.query_awaitable("SELECT 1; SELECT 1/0; SELECT 3");
        CHECK(!e.ok && e.err_detail.sqlstate == "22012" && e.rows.empty());

        // A slow second statement must not block the worker thread: a timer
        // on the same worker keeps ticking meanwhile.
        std::atomic<int> ticks{0};
        auto ticker = [](std::atomic<int> *t) -> task::Awaitable<void> {
            for (int i = 0; i < 10; ++i) {
                if (!co_await system::this_coroutine::sleep_for(20ms)) co_return;
                t->fetch_add(1);
            }
        };
        system::co_spawn_static(ticker(&ticks), 0);  // the worker this case runs on
        auto slow = co_await pool.query_awaitable("SELECT 1; SELECT pg_sleep(0.3)");
        CHECK(slow.ok);
        CHECK_MSG(ticks.load() >= 5, std::to_string(ticks.load()));
    }

    task::Awaitable<void> t_concurrency() {
        auto &pool = *pg_pool(8);
        std::atomic<int> ok{0}, done{0};
        auto worker = [](PgPool *p, std::atomic<int> *okc, std::atomic<int> *d, int base) -> task::Awaitable<void> {
            for (int i = 0; i < 100; ++i) {
                auto r = co_await p->query_reflect_expected_one<IntOnly>("SELECT $1::bigint AS v",
                                                                          int64_t{base + i});
                if (r && r->v == base + i) okc->fetch_add(1);
            }
            d->fetch_add(1);
        };
        for (int w = 0; w < 32; ++w) system::co_spawn(worker(&pool, &ok, &done, w * 1000));
        while (done.load() < 32) co_await system::this_coroutine::sleep_for(10ms);
        CHECK_MSG(ok.load() == 3200, std::to_string(ok.load()));
    }

    // ---------- cases: transactions ----------

#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_transactions() {
        auto &pool = *pg_pool(2);
        const std::string table = unique("upq_tx");
        CHECK((co_await pool.query_awaitable("CREATE TABLE " + table + " (id int primary key, v text)")).ok);

        {
            PgTransaction tx(&pool);
            CHECK(co_await tx.begin());
            CHECK((co_await tx.query("INSERT INTO " + table + " VALUES ($1, $2)", 1, std::string("a"))).ok);
            co_await tx.rollback();
            CHECK(tx.is_rolled_back());
        }
        {
            PgTransaction tx(&pool, {.isolation = TxIsolationLevel::ReadCommitted});
            CHECK(!(co_await tx.begin_errored()).has_value());
            CHECK((co_await tx.query("INSERT INTO " + table + " VALUES ($1, $2)", 2, std::string("b"))).ok);
            auto rows = co_await tx.query_reflect_expected<IntOnly>("SELECT id::bigint AS v FROM " + table);
            CHECK(rows && rows->size() == 1);
            CHECK(co_await tx.commit());
        }
        {
            // Reuse of one object for a second transaction (RNC does this).
            PgTransaction tx(&pool);
            CHECK(co_await tx.begin());
            co_await tx.finish();
            CHECK(co_await tx.begin());
            CHECK((co_await tx.query("INSERT INTO " + table + " VALUES (3, 'c')")).ok);
            CHECK(co_await tx.commit());
        }
        {
            // Emulated read-only: no BEGIN, statements autocommit.
            PgTransaction tx(&pool, {.read_only = true});
            CHECK(co_await tx.begin());
            auto n = co_await tx.query_reflect_expected_one<IntOnly>("SELECT count(*)::bigint AS v FROM " + table);
            CHECK(n && n->v == 2);
            CHECK(co_await tx.commit());
        }
        {
            // A failed statement aborts the transaction; COMMIT then only rolls back.
            PgTransaction tx(&pool);
            CHECK(co_await tx.begin());
            CHECK((co_await tx.query("INSERT INTO " + table + " VALUES (10, 'x')")).ok);
            auto dup = co_await tx.query("INSERT INTO " + table + " VALUES (2, 'dup')");
            CHECK(!dup.ok && dup.err_detail.sqlstate == "23505");
            CHECK(!(co_await tx.commit()));
            CHECK(tx.last_commit_error() && tx.last_commit_error()->err_detail.sqlstate == "25P02");
        }
        {
            PgTransaction tx(&pool);
            CHECK(co_await tx.begin());
            auto sp = tx.make_subtx();
            CHECK(co_await sp.begin());
            CHECK((co_await sp.query("INSERT INTO " + table + " VALUES (20, 'sp')")).ok);
            co_await sp.rollback();
            CHECK((co_await tx.query("INSERT INTO " + table + " VALUES (21, 'kept')")).ok);
            CHECK(co_await tx.commit());
        }
        auto ids = co_await pool.query_reflect_expected<IntOnly>("SELECT id::bigint AS v FROM " + table + " ORDER BY id");
        CHECK(ids.has_value());
        std::string got;
        for (auto &r : *ids) got += std::to_string(r.v) + ",";
        CHECK_MSG(got == "2,3,21,", got);
        co_await pool.query_awaitable("DROP TABLE " + table);
    }

#endif
    // ---------- cases: cursors, COPY ----------

    task::Awaitable<void> t_stream_reflect() {
        auto &pool = *pg_pool(2);
        PgTransaction tx(&pool, {.read_only = true, .force_real_begin = true});
        CHECK(co_await tx.begin());
        auto s = co_await tx.stream_reflect<NamedRow>(
            "SELECT g::bigint AS id, 'r' || g AS name, NULL::text AS note, g * 0.5 AS score "
            "FROM generate_series(1, $1) g ORDER BY g",
            1000, int32_t{2500});
        CHECK(s.has_value());
        int64_t n = 0, sum = 0;
        while (auto row = co_await s->next()) {
            ++n;
            sum += row->id;
        }
        CHECK(s->ok());
        co_await s->close();
        CHECK_MSG(n == 2500 && sum == 2500LL * 2501 / 2, std::to_string(n));
        CHECK(co_await tx.commit());

        auto ps = co_await pool.stream("SELECT g FROM generate_series(1, 10) g", 3);
        CHECK(ps.has_value());
        int m = 0;
        while (auto row = co_await ps->next()) ++m;
        co_await ps->close();
        CHECK(m == 10);
    }

    task::Awaitable<void> t_copy() {
        auto &pool = *pg_pool(2);
        const std::string table = unique("upq_copy");
        CHECK((co_await pool.query_awaitable("CREATE TABLE " + table + " (id int, v text)")).ok);

        const std::string data = "1\ta\n2\tb\n3\tc\n";
        auto in = co_await pool.copy_from_buffer("COPY " + table + " FROM STDIN", data.data(), data.size());
        CHECK_MSG(in.ok, in.error);
        CHECK(in.rows_affected == 3);

        std::vector<std::string> lines;
        auto out = co_await pool.copy_to_lines("COPY " + table + " TO STDOUT",
                                               [&](std::string_view l) -> task::Awaitable<bool> {
                                                   lines.emplace_back(l);
                                                   co_return true;
                                               });
        CHECK_MSG(out.ok, out.error);
        CHECK(lines.size() == 3 && lines[2] == "3\tc");

        // Server error at the end of COPY IN: before the refactor this path
        // wrote a QueryResult over a PgCopyResult (out-of-bounds).
        const std::string bad = "x\ty\n";
        auto err = co_await pool.copy_from_buffer("COPY " + table + " FROM STDIN", bad.data(), bad.size());
        CHECK(!err.ok);
        CHECK_MSG(err.err_detail.sqlstate == "22P02", err.error);
        CHECK(err.error.find("[SQLSTATE 22P02]") != std::string::npos);

        // COPY through a query API is refused instead of looping forever.
        auto q = co_await pool.query_awaitable("COPY " + table + " TO STDOUT");
        CHECK(!q.ok && q.code == PgErrorCode::ProtocolCorrupt);
        CHECK((co_await pool.query_awaitable("SELECT 1")).ok);
        co_await pool.query_awaitable("DROP TABLE " + table);
    }

    // ---------- cases: socket ownership ----------

    // libpq owns the descriptor of every connection; nobody else may close
    // it. When the server ends an idle pooled session (restart, pooler idle
    // timeout, pg_terminate_backend) the FIN reaches a socket nobody waits
    // on. If the event loop closed the fd at that point, libpq would keep
    // using a number the kernel hands to the next socket the process opens:
    // the next statement would be written into that stranger, and the IO
    // watchdog would later shut the stranger down.
    task::Awaitable<void> t_server_close_keeps_fd_ownership() {
        auto &pool = *pg_pool(1);
        auto &admin = *pg_pool(1);
        const int64_t pid = co_await backend_pid(pool);
        CHECK(pid > 0);
        int fd = -1;
        {
            auto c = co_await pool.acquire_connection();
            CHECK(c && *c);
            fd = PQsocket((*c)->raw_conn());
            pool.release_connection(*c);
        }
        CHECK(fd >= 0);
        CHECK((co_await admin.query_awaitable("SELECT pg_terminate_backend($1::int)", pid)).ok);
        co_await system::this_coroutine::sleep_for(200ms);

        const bool fd_open = ::fcntl(fd, F_GETFD) != -1;

        // Take the lowest free numbers, as the next accept/connect would.
        int sv[2];
        CHECK(::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv) == 0);
        const int peer = sv[0] == fd ? sv[1] : sv[1] == fd ? sv[0] : -1;

        auto r = co_await pool.query_awaitable(QueryOpts{.io_deadline_ms = 2000}, "SELECT 1");
        char buf[512];
        const ssize_t leaked = peer >= 0 ? ::recv(peer, buf, sizeof buf, MSG_DONTWAIT) : -1;
        ::close(sv[0]);
        ::close(sv[1]);

        CHECK_MSG(fd_open, "fd " + std::to_string(fd) + " was closed behind libpq's back");
        CHECK_MSG(leaked <= 0, std::to_string(leaked) + " bytes of the statement went into a foreign socket");
        CHECK_MSG(r.ok, r.error);
    }

    // Closing a connection must release its descriptor exactly once. The
    // pool closes connections on whatever worker notices they are dead,
    // usually not the one that opened them; a second close of the same
    // number, deferred to the opening worker, would hit whichever socket the
    // process opened in between (the kernel hands out the lowest free
    // number, so the very next socket gets it).
    struct CloseProbe {
        std::shared_ptr<PgConnectionLibpq> conn;
        std::atomic<int> state{0};  // 0 running, 1 clean, 2 number reused but intact, 3 stolen
        std::string detail;
    };

    task::Awaitable<void> close_and_take_number(CloseProbe *p) {
        const int fd = PQsocket(p->conn->raw_conn());
        p->conn->close();
        int sv[2];
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv) != 0) {
            p->detail = "socketpair failed";
            p->state = 3;
            co_return;
        }
        const bool reused = sv[0] == fd || sv[1] == fd;
        co_await system::this_coroutine::sleep_for(50ms);  // let the opening worker run its queue
        const bool intact = ::fcntl(sv[0], F_GETFD) != -1 && ::fcntl(sv[1], F_GETFD) != -1;
        if (!intact)
            p->detail = "fd " + std::to_string(fd) + " closed again after it was handed to a new socket";
        ::close(sv[0]);
        ::close(sv[1]);
        p->state = !intact ? 3 : reused ? 2 : 1;
    }

    task::Awaitable<void> t_close_releases_fd_once() {
        auto ci = make_conninfo(g_pg->host, g_pg->port, g_pg->user, g_pg->db, g_pg->password,
                                SSLConfig{.mode = SSLMode::disable}, {});
        CHECK(ci.has_value());
        int reused = 0;
        for (int round = 0; round < 20; ++round) {
            CloseProbe probe;
            probe.conn = std::make_shared<PgConnectionLibpq>();
            auto err = co_await probe.conn->connect_async(*ci);  // registered on this worker (0)
            CHECK_MSG(!err, err.value_or(""));
            system::co_spawn_static(close_and_take_number(&probe), 1);
            while (probe.state.load() == 0) co_await system::this_coroutine::sleep_for(5ms);
            CHECK_MSG(probe.state.load() != 3, "round " + std::to_string(round) + ": " + probe.detail);
            if (probe.state.load() == 2) ++reused;
        }
        CHECK_MSG(reused > 0, "the freed number was never reused, the case proved nothing");
    }

    // ---------- cases: retries against a live server ----------

#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_stale_idle_connection_dropped() {
        auto &pool = *pg_pool(1);
        auto &admin = *pg_pool(1);
        const int64_t pid = co_await backend_pid(pool);
        CHECK(pid > 0);
        CHECK((co_await admin.query_awaitable("SELECT pg_terminate_backend($1::int)", pid)).ok);
        co_await system::this_coroutine::sleep_for(100ms);

        // No retry flags: the dead idle connection is noticed before the
        // statement is sent, so even a write would be safe.
        auto r = co_await pool.query_awaitable("SELECT 1");
        CHECK_MSG(r.ok, r.error);
        CHECK(pool.health_stats().stale_dropped.load() == 1);
        CHECK(co_await backend_pid(pool) != pid);
    }

#endif
#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_connection_lost_mid_statement() {
        auto &pool = *pg_pool(1);
        auto &admin = *pg_pool(1);
        const int64_t pid = co_await backend_pid(pool);

        auto killer = [](PgPool *a, int64_t victim) -> task::Awaitable<void> {
            co_await system::this_coroutine::sleep_for(150ms);
            co_await a->query_awaitable("SELECT pg_terminate_backend($1::int)", victim);
        };

        // Not idempotent: the loss is reported.
        system::co_spawn(killer(&admin, pid));
        auto plain = co_await pool.query_awaitable("SELECT pg_sleep(0.5)");
        CHECK(!plain.ok);
        CHECK_MSG(classify_retry(plain) == RetryClass::ConnectionLost, plain.error);

        // Idempotent: re-run once on a fresh connection.
        const int64_t pid2 = co_await backend_pid(pool);
        system::co_spawn(killer(&admin, pid2));
        auto retried = co_await pool.query_awaitable(QueryOpts{.idempotent = true}, "SELECT pg_sleep(0.5)");
        CHECK_MSG(retried.ok, retried.error);
        CHECK(pool.health_stats().connection_loss_retries.load() == 1);
    }

#endif
#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_deadline_retry() {
        auto &pool = *pg_pool(2);
        const std::string seq = unique("upq_seq");
        CHECK((co_await pool.query_awaitable("CREATE SEQUENCE " + seq)).ok);
        const std::string sql = "SELECT pg_sleep(CASE WHEN nextval('" + seq + "') = 1 THEN 4 ELSE 0 END)";

        auto plain = co_await pool.query_awaitable(QueryOpts{.io_deadline_ms = 1000}, sql);
        CHECK(!plain.ok);
        CHECK_MSG(plain.error.starts_with("io deadline exceeded"), plain.error);
        CHECK(classify_retry(plain) == RetryClass::Deadline);

        CHECK((co_await pool.query_awaitable("SELECT setval('" + seq + "', 1, false)")).ok);
        auto retried = co_await pool.query_awaitable(QueryOpts{.io_deadline_ms = 1000, .idempotent = true}, sql);
        CHECK_MSG(retried.ok, retried.error);
        CHECK(pool.health_stats().deadline_retries.load() == 1);
        co_await pool.query_awaitable("DROP SEQUENCE " + seq);
    }

#endif
#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_rolled_back_retry_and_budget() {
        auto &pool = *pg_pool(3);
        const std::string table = unique("upq_lock");
        CHECK((co_await pool.query_awaitable("CREATE TABLE " + table + " (id int primary key)")).ok);
        CHECK((co_await pool.query_awaitable("INSERT INTO " + table + " VALUES (1)")).ok);
        const std::string grab = "SELECT id FROM " + table + " WHERE id = 1 FOR UPDATE NOWAIT";

        auto hold = [](PgPool *p, std::string t, int ms) -> task::Awaitable<void> {
            PgTransaction tx(p);
            if (!co_await tx.begin()) co_return;
            co_await tx.query("SELECT id FROM " + t + " WHERE id = 1 FOR UPDATE");
            co_await system::this_coroutine::sleep_for(std::chrono::milliseconds(ms));
            co_await tx.commit();
        };

        // Lock held ~120 ms; retries back off 50 + 100 ms, so the third try wins.
        system::co_spawn(hold(&pool, table, 120));
        co_await system::this_coroutine::sleep_for(30ms);
        auto r = co_await pool.query_awaitable(QueryOpts{.idempotent = true}, grab);
        CHECK_MSG(r.ok, r.error);
        CHECK(pool.health_stats().rollback_retries.load() >= 1);

        // Without opting in: reported as is.
        system::co_spawn(hold(&pool, table, 200));
        co_await system::this_coroutine::sleep_for(30ms);
        auto plain = co_await pool.query_awaitable(grab);
        CHECK(!plain.ok && plain.err_detail.sqlstate == "55P03");
        co_await system::this_coroutine::sleep_for(250ms);

        // A budget smaller than the first backoff: no retry at all.
        const uint64_t before = pool.health_stats().rollback_retries.load();
        system::co_spawn(hold(&pool, table, 200));
        co_await system::this_coroutine::sleep_for(30ms);
        auto budgeted = co_await pool.query_awaitable(QueryOpts{.idempotent = true, .retry_budget_ms = 40}, grab);
        CHECK(!budgeted.ok);
        CHECK(pool.health_stats().rollback_retries.load() == before);
        co_await system::this_coroutine::sleep_for(250ms);
        co_await pool.query_awaitable("DROP TABLE " + table);
    }

#endif
#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_run_tx() {
        auto &pool = *pg_pool(3);
        const std::string table = unique("upq_runtx");
        CHECK((co_await pool.query_awaitable("CREATE TABLE " + table + " (id int primary key, n int)")).ok);
        CHECK((co_await pool.query_awaitable("INSERT INTO " + table + " VALUES (1, 0)")).ok);

        std::vector<RetryEvent> events;
        pool.set_retry_observer([&](const RetryEvent &ev) { events.push_back(ev); });

        // Attempt 1 kills its own backend mid-transaction: the whole
        // transaction is re-run and commits exactly once.
        int calls = 0;
        auto r = co_await run_tx(pool, [&](PgTransaction &tx) -> task::Awaitable<std::expected<int64_t, PgOpError> > {
            ++calls;
            auto up = co_await tx.query("UPDATE " + table + " SET n = n + 1 WHERE id = 1");
            if (!up.ok) co_return std::unexpected(usub::pg::detail::to_op_error(up));
            if (calls == 1) {
                auto kill = co_await tx.query("SELECT pg_terminate_backend(pg_backend_pid())");
                if (!kill.ok) co_return std::unexpected(usub::pg::detail::to_op_error(kill));
            }
            auto n = co_await tx.query_reflect_expected_one<IntOnly>(
                "SELECT n::bigint AS v FROM " + table + " WHERE id = 1");
            if (!n) co_return std::unexpected(n.error());
            co_return n->v;
        });
        CHECK_MSG(r.has_value(), r ? "" : r.error().error);
        CHECK(*r == 1 && calls == 2);
        CHECK(events.size() == 1 && events[0].cls == RetryClass::ConnectionLost && events[0].in_transaction);

        // A business error from the body is returned as is, without retries.
        calls = 0;
        auto biz = co_await run_tx(pool, [&](PgTransaction &) -> task::Awaitable<std::expected<int, PgOpError> > {
            ++calls;
            co_return std::unexpected(PgOpError{PgErrorCode::Unknown, "no rows", {}});
        });
        CHECK(!biz && biz.error().error == "no rows" && calls == 1);

        // An exception rolls back and propagates.
        bool threw = false;
        try {
            auto x = co_await run_tx(pool, [&](PgTransaction &tx) -> task::Awaitable<std::expected<int, PgOpError> > {
                co_await tx.query("UPDATE " + table + " SET n = 100 WHERE id = 1");
                throw std::runtime_error("boom");
            });
            (void) x;
        } catch (const std::runtime_error &) {
            threw = true;
        }
        CHECK(threw);
        auto n = co_await pool.query_reflect_expected_one<IntOnly>("SELECT n::bigint AS v FROM " + table);
        CHECK(n && n->v == 1);
        co_await pool.query_awaitable("DROP TABLE " + table);
    }

#endif
    task::Awaitable<void> t_cancel_statement() {
        auto &pool = *pg_pool(1);
        auto sleeper = [](PgPool *p) -> task::Awaitable<QueryResult> {
            co_return co_await p->query_awaitable("SELECT pg_sleep(3)");
        };
        auto h = task::spawn(sleeper(&pool));
        co_await system::this_coroutine::sleep_for(200ms);
        const auto t0 = std::chrono::steady_clock::now();
        h.cancel();
        const QueryResult r = co_await h;
        const auto waited = std::chrono::steady_clock::now() - t0;
        CHECK_MSG(!r.ok && r.code == PgErrorCode::AwaitCanceled, r.error);
        CHECK(waited < 1s);
        // The abandoned connection was retired, not handed out again.
        auto again = co_await pool.query_awaitable("SELECT 1");
        CHECK_MSG(again.ok, again.error);
    }

    task::Awaitable<void> t_notifications() {
        auto &pool = *pg_pool(2);
        auto c = co_await pool.acquire_connection();
        CHECK(c.has_value());

        struct Handler : IPgNotifyHandler {
            std::atomic<int> *got;
            explicit Handler(std::atomic<int> *g) : got(g) {}
            task::Awaitable<void> operator()(std::string, std::string payload, int) override {
                if (payload == "ping") got->fetch_add(1);
                co_return;
            }
        };
        std::atomic<int> got{0};
        auto *mux = new PgNotificationMultiplexer(*c, g_pg->host, g_pg->port, g_pg->user, g_pg->db,
                                                  g_pg->password);
        const std::string channel = unique("upq_chan");
        auto h = co_await mux->add_handler(channel, std::make_shared<Handler>(&got));
        CHECK(h.has_value());
        system::co_spawn(mux->run());
        for (int i = 0; i < 3; ++i)
            co_await pool.query_awaitable("SELECT pg_notify($1, 'ping')", channel);
        for (int i = 0; i < 100 && got.load() < 3; ++i) co_await system::this_coroutine::sleep_for(10ms);
        CHECK_MSG(got.load() == 3, std::to_string(got.load()));
    }

    // Subscriptions change while run() owns the connection, the listener's
    // backend dies, and nothing arrives for a while.
    struct CountingHandler : IPgNotifyHandler {
        std::atomic<int> *got;
        explicit CountingHandler(std::atomic<int> *g) : got(g) {}
        task::Awaitable<void> operator()(std::string, std::string, int) override {
            got->fetch_add(1);
            co_return;
        }
    };

    double cpu_ms() {
        rusage ru{};
        getrusage(RUSAGE_SELF, &ru);
        return (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1e3 + (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) / 1e3;
    }

    task::Awaitable<bool> wait_count(std::atomic<int> &n, int want, std::chrono::milliseconds limit) {
        for (auto waited = 0ms; n.load() < want && waited < limit; waited += 10ms)
            co_await system::this_coroutine::sleep_for(10ms);
        co_return n.load() >= want;
    }

    task::Awaitable<void> t_notifications_live_changes() {
        auto &pool = *pg_pool(2);
        auto c = co_await pool.acquire_connection();
        CHECK(c.has_value());
        const int listener_pid = PQbackendPID((*c)->raw_conn());
        auto *mux = new PgNotificationMultiplexer(*c, g_pg->host, g_pg->port, g_pg->user, g_pg->db,
                                                  g_pg->password);
        std::atomic<int> a{0}, b{0};
        const std::string ch_a = unique("upq_live_a"), ch_b = unique("upq_live_b");
        CHECK((co_await mux->add_handler(ch_a, std::make_shared<CountingHandler>(&a))).has_value());
        system::co_spawn(mux->run());
        co_await system::this_coroutine::sleep_for(50ms);  // run loop parked on the socket

        // Idle: nothing may spin while nothing arrives.
        const double cpu0 = cpu_ms();
        co_await system::this_coroutine::sleep_for(1000ms);
        const double idle_ms = cpu_ms() - cpu0;  // checked last, so the other parts run too
        std::fprintf(stderr, "    idle CPU with one channel: %.1f ms per second\n", idle_ms);

        // Subscribe while run() owns the connection.
        auto hb = co_await mux->add_handler(ch_b, std::make_shared<CountingHandler>(&b));
        CHECK(hb.has_value());
        co_await pool.query_awaitable("SELECT pg_notify($1, 'x'), pg_notify($2, 'x')", ch_a, ch_b);
        CHECK(co_await wait_count(a, 1, 2000ms));
        CHECK(co_await wait_count(b, 1, 2000ms));

        // Unsubscribe while running: b goes quiet, a keeps working.
        CHECK(mux->remove_handler(*hb));
        co_await pool.query_awaitable("SELECT pg_notify($1, 'x'), pg_notify($2, 'x')", ch_b, ch_a);
        CHECK(co_await wait_count(a, 2, 2000ms));
        CHECK_MSG(b.load() == 1, std::to_string(b.load()));

        // The listener's backend dies: reconnect, LISTEN again, deliver again.
        CHECK((co_await pool.query_awaitable("SELECT pg_terminate_backend($1::int)", listener_pid)).ok);
        bool delivered = false;
        for (int i = 0; i < 50 && !delivered; ++i) {
            co_await pool.query_awaitable("SELECT pg_notify($1, 'x')", ch_a);
            delivered = co_await wait_count(a, 3, 100ms);
        }
        CHECK_MSG(delivered, "no delivery after the listener backend was terminated");
        CHECK_MSG(b.load() == 1, std::to_string(b.load()));
        CHECK_MSG(idle_ms < 20, "idle CPU " + std::to_string(idle_ms) + " ms per second");
    }

    // A burst far above the old admission limits (1000/s per channel,
    // channel_queue_capacity in flight): every notification is delivered,
    // in order, and no more handler runs overlap than the channel allows.
    struct RecordingHandler : IPgNotifyHandler {
        std::mutex *mu;
        std::vector<int> *seen;
        std::atomic<int> *running;
        std::atomic<int> *peak;
        std::chrono::milliseconds hold;

        RecordingHandler(std::mutex *m, std::vector<int> *s, std::atomic<int> *r, std::atomic<int> *p,
                         std::chrono::milliseconds h)
            : mu(m), seen(s), running(r), peak(p), hold(h) {}

        task::Awaitable<void> operator()(std::string, std::string payload, int) override {
            const int now = running->fetch_add(1) + 1;
            int p = peak->load();
            while (now > p && !peak->compare_exchange_weak(p, now)) {}
            {
                std::lock_guard<std::mutex> lk(*mu);
                seen->push_back(std::stoi(payload));
            }
            if (hold.count() > 0) co_await system::this_coroutine::sleep_for(hold);
            running->fetch_sub(1);
        }
    };

    task::Awaitable<void> t_notifications_burst_not_dropped() {
        auto &pool = *pg_pool(4);  // one connection per multiplexer below, one for the NOTIFYs
        struct Run {
            size_t limit;
            int count;
            std::chrono::milliseconds hold;
        };
        for (const Run run : {Run{2048, 3000, 0ms}, Run{4, 200, 5ms}, Run{1, 2000, 0ms}}) {
            auto c = co_await pool.acquire_connection();
            CHECK(c.has_value());
            auto *mux = new PgNotificationMultiplexer(*c, g_pg->host, g_pg->port, g_pg->user, g_pg->db,
                                                      g_pg->password, PgNotificationMultiplexer::Config{run.limit});
            auto *mu = new std::mutex;
            auto *seen = new std::vector<int>;
            auto *running = new std::atomic<int>{0};
            auto *peak = new std::atomic<int>{0};
            const std::string ch = unique("upq_burst");
            CHECK((co_await mux->add_handler(ch, std::make_shared<RecordingHandler>(mu, seen, running, peak,
                                                                                     run.hold))).has_value());
            system::co_spawn(mux->run());
            CHECK((co_await pool.query_awaitable("SELECT count(pg_notify($1, g::text)) FROM generate_series(1, $2) g",
                                                 ch, run.count)).ok);
            size_t got = 0, before = 0;
            for (int idle = 0; idle < 200; ++idle) {  // give up after 2 s without progress
                {
                    std::lock_guard<std::mutex> lk(*mu);
                    got = seen->size();
                }
                if (got >= static_cast<size_t>(run.count)) break;
                if (got != before) idle = 0;
                before = got;
                co_await system::this_coroutine::sleep_for(10ms);
            }
            const auto st = mux->stats();
            CHECK_MSG(got == static_cast<size_t>(run.count),
                      "limit " + std::to_string(run.limit) + ": delivered " + std::to_string(got) + " of " +
                      std::to_string(run.count) + ", dropped overflow=" + std::to_string(st.dropped_overflow) +
                      " rate=" + std::to_string(st.dropped_rate_limited));
            CHECK_MSG(peak->load() <= static_cast<int>(run.limit), "peak " + std::to_string(peak->load()));
            if (run.limit == 1) {
                std::lock_guard<std::mutex> lk(*mu);
                for (size_t i = 0; i < seen->size(); ++i)
                    CHECK_MSG((*seen)[i] == static_cast<int>(i) + 1, "out of order at " + std::to_string(i));
            } else if (run.hold.count() > 0) {
                CHECK_MSG(peak->load() > 1, "handlers never overlapped");
            }
        }
    }

    // ---------- cases: fake server (retry matrix) ----------

#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_fake_not_started() {
        auto &pool = *fake_pool(unique("notstarted_2"));
        std::vector<RetryEvent> events;
        pool.set_retry_observer([&](const RetryEvent &ev) { events.push_back(ev); });
        auto r = co_await pool.query_awaitable("SELECT 1");  // simple protocol
        CHECK_MSG(r.ok, r.error);
        CHECK(events.size() == 2 && events[0].cls == RetryClass::NotStarted && events[1].attempt == 2);
        CHECK(pool.health_stats().transient_retries.load() == 2);

        auto &pool2 = *fake_pool(unique("notstarted_5"));
        auto e = co_await pool2.query_awaitable("SELECT $1::int", 1);  // extended protocol
        CHECK(!e.ok && e.err_detail.sqlstate == "08P01");
        CHECK(pool2.health_stats().transient_retries.load() == 2);  // 3 attempts in total
    }

#endif
#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_fake_drop() {
        auto &plain = *fake_pool(unique("drop_1"));
        auto r = co_await plain.query_awaitable("SELECT 1");
        CHECK(!r.ok && classify_retry(r) == RetryClass::ConnectionLost);

        auto &legacy = *fake_pool(unique("drop_1"));
        auto l = co_await legacy.query_awaitable(QueryOpts{.retry_on_connection_loss = true}, "SELECT 1");
        CHECK_MSG(l.ok, l.error);

        auto &twice = *fake_pool(unique("drop_2"));
        auto t = co_await twice.query_awaitable(QueryOpts{.idempotent = true}, "SELECT 1");
        CHECK(!t.ok);  // one re-run per lost connection
        CHECK(twice.health_stats().connection_loss_retries.load() == 1);
    }

#endif
#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_fake_hang() {
        auto &pool = *fake_pool(unique("hang_1"));
        auto r = co_await pool.query_reflect_expected<IntOnly>(QueryOpts{.io_deadline_ms = 1000, .idempotent = true},
                                                               "SELECT $1::int AS v", 1);
        CHECK_MSG(r.has_value(), r ? "" : r.error().error);
        CHECK(pool.health_stats().deadline_retries.load() == 1);
        CHECK(pool.health_stats().io_watchdog_aborts.load() == 1);
    }

#endif
#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_fake_deadlock() {
        auto &pool = *fake_pool(unique("deadlock_2"));
        auto r = co_await pool.query_reflect_expected_one<IntOnly>(QueryOpts{.idempotent = true}, "SELECT 1 AS v");
        CHECK(r.has_value() && r->v == 1);
        CHECK(pool.health_stats().rollback_retries.load() == 2);
    }

#endif
    // ---------- pure ----------

#ifndef UPQ_TESTS_BASELINE
    task::Awaitable<void> t_classifier() {
        auto qr = [](PgErrorCode code, std::string sqlstate, std::string err) {
            QueryResult r;
            r.ok = false;
            r.code = code;
            r.error = std::move(err);
            r.err_detail.sqlstate = std::move(sqlstate);
            return r;
        };
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "08P01", "x")) == RetryClass::NotStarted);
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "53300", "x")) == RetryClass::NotStarted);
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "08006", "x")) == RetryClass::ConnectionLost);
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "25P03", "x")) == RetryClass::ConnectionLost);
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "57P01", "x")) == RetryClass::ConnectionLost);
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "40001", "x")) == RetryClass::RolledBack);
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "55P03", "x")) == RetryClass::RolledBack);
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "23505", "x")) == RetryClass::None);
        CHECK(classify_retry(qr(PgErrorCode::ServerError, "", "SSL connection has been closed unexpectedly")) ==
              RetryClass::ConnectionLost);
        CHECK(classify_retry(qr(PgErrorCode::SocketReadFailed, "", "io deadline exceeded (waited_ms=1)")) ==
              RetryClass::Deadline);
        CHECK(classify_retry(qr(PgErrorCode::SocketReadFailed, "", "x"), true) == RetryClass::Deadline);
        CHECK(classify_retry(qr(PgErrorCode::ConnectionClosed, "", "connection not OK")) ==
              RetryClass::ConnectionLost);
        CHECK(classify_retry(qr(PgErrorCode::AwaitCanceled, "", "x")) == RetryClass::None);
        CHECK(classify_retry(qr(PgErrorCode::InvalidFuture, "", "transaction not active")) == RetryClass::None);
        CHECK(classify_retry(qr(PgErrorCode::Unknown, "", "no rows")) == RetryClass::None);

        PgPool pool("127.0.0.1", "1", "u", "d", "p");
        using S = PgPool::RetryScope;
        {
            PgPool::RetryBudget b(QueryOpts{});
            CHECK(!pool.next_retry_delay(b, RetryClass::ConnectionLost));
            CHECK(!pool.next_retry_delay(b, RetryClass::RolledBack));
            CHECK(pool.next_retry_delay(b, RetryClass::NotStarted) == 200u);
            b.attempt = 1;
            CHECK(pool.next_retry_delay(b, RetryClass::NotStarted) == 600u);
            b.attempt = 2;
            CHECK(!pool.next_retry_delay(b, RetryClass::NotStarted));
        }
        {
            PgPool::RetryBudget b(QueryOpts{});
            CHECK(pool.next_retry_delay(b, RetryClass::ConnectionLost, S::Transaction) == 0u);
            CHECK(!pool.next_retry_delay(b, RetryClass::ConnectionLost, S::Transaction));  // once
            CHECK(!pool.next_retry_delay(b, RetryClass::Deadline, S::TransactionCommit));
            CHECK(pool.next_retry_delay(b, RetryClass::RolledBack, S::TransactionCommit) == 50u);
        }
        {
            PgPool::RetryBudget b(QueryOpts{.idempotent = true});
            CHECK(pool.next_retry_delay(b, RetryClass::Deadline, S::TransactionCommit) == 0u);
        }
        co_return;
    }

#endif
#ifndef UPQ_TESTS_BASELINE
    // The handshake waits on the socket (no polling) and is still bounded:
    // a server that accepts TCP and never answers costs the connect budget,
    // not forever.
    task::Awaitable<void> t_fake_connect_deadline() {
        auto ci = make_conninfo("127.0.0.1", *g_fake_port, "u", "mute_1_connect", "",
                                SSLConfig{.mode = SSLMode::disable}, {});
        CHECK(ci.has_value());
        auto conn = std::make_shared<PgConnectionLibpq>();
        const auto t0 = std::chrono::steady_clock::now();
        auto err = co_await conn->connect_async(*ci, 400ms);
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0);
        CHECK(err.has_value());
        CHECK_MSG(err->find("connect timeout") != std::string::npos, *err);
        CHECK_MSG(ms >= 350ms && ms < 2000ms, std::to_string(ms.count()) + " ms");
        CHECK(!conn->connected());
    }

    // A refused TCP connect keeps libpq's own diagnosis: the poller must not
    // swallow the socket error before libpq reads it.
    task::Awaitable<void> t_connect_refused_message() {
        auto ci = make_conninfo("127.0.0.1", "1", "u", "d", "", SSLConfig{.mode = SSLMode::disable}, {});
        CHECK(ci.has_value());
        auto conn = std::make_shared<PgConnectionLibpq>();
        auto err = co_await conn->connect_async(*ci, 2000ms);
        CHECK(err.has_value());
        CHECK_MSG(err->find("refused") != std::string::npos, *err);
    }

#endif
    // ---------- runner ----------

    struct Case {
        const char *name;
        task::Awaitable<void> (*fn)();
        char needs;  // 'p' live PG, 'f' fake server, '-' nothing
    };

    const Case kCases[] = {
#ifndef UPQ_TESTS_BASELINE
        {"classifier", &t_classifier, '-'},
#endif
        {"simple_query", &t_simple_query, 'p'},
        {"param_types", &t_param_types, 'p'},
        {"reflect_named_and_positional", &t_reflect_named_and_positional, 'p'},
        {"no_rows_and_errors_keep_connection", &t_no_rows_and_errors_keep_connection, 'p'},
        {"mapping_exception_releases_connection", &t_mapping_exception_releases_connection, 'p'},
        {"multi_statement", &t_multi_statement, 'p'},
        {"concurrency", &t_concurrency, 'p'},
#ifndef UPQ_TESTS_BASELINE
        {"transactions", &t_transactions, 'p'},
#endif
        {"stream_reflect", &t_stream_reflect, 'p'},
        {"copy", &t_copy, 'p'},
        {"server_close_keeps_fd_ownership", &t_server_close_keeps_fd_ownership, 'p'},
        {"close_releases_fd_once", &t_close_releases_fd_once, 'p'},
#ifndef UPQ_TESTS_BASELINE
        {"stale_idle_connection_dropped", &t_stale_idle_connection_dropped, 'p'},
#endif
#ifndef UPQ_TESTS_BASELINE
        {"connection_lost_mid_statement", &t_connection_lost_mid_statement, 'p'},
#endif
#ifndef UPQ_TESTS_BASELINE
        {"deadline_retry", &t_deadline_retry, 'p'},
#endif
#ifndef UPQ_TESTS_BASELINE
        {"rolled_back_retry_and_budget", &t_rolled_back_retry_and_budget, 'p'},
#endif
#ifndef UPQ_TESTS_BASELINE
        {"run_tx", &t_run_tx, 'p'},
#endif
        {"cancel_statement", &t_cancel_statement, 'p'},
        {"notifications", &t_notifications, 'p'},
        {"notifications_live_changes", &t_notifications_live_changes, 'p'},
        {"notifications_burst_not_dropped", &t_notifications_burst_not_dropped, 'p'},
#ifndef UPQ_TESTS_BASELINE
        {"fake_not_started", &t_fake_not_started, 'f'},
#endif
#ifndef UPQ_TESTS_BASELINE
        {"fake_drop", &t_fake_drop, 'f'},
#endif
#ifndef UPQ_TESTS_BASELINE
        {"fake_hang", &t_fake_hang, 'f'},
#endif
#ifndef UPQ_TESTS_BASELINE
        {"fake_deadlock", &t_fake_deadlock, 'f'},
        {"fake_connect_deadline", &t_fake_connect_deadline, 'f'},
        {"connect_refused_message", &t_connect_refused_message, '-'},
#endif
    };

    std::atomic<bool> g_done{false};

    task::Awaitable<void> run_case(task::Awaitable<void> (*fn)(), usub::Uvent *rt) {
        try {
            co_await fn();
        } catch (const std::exception &e) {
            report_failure(__FILE__, __LINE__, std::string("exception: ") + e.what());
        }
        g_done = true;
        rt->stop();
    }

    int run_in_child(const Case &c) {
        usub::Uvent rt(2);
        std::thread([] {
            for (int i = 0; i < 300 && !g_done.load(); ++i) std::this_thread::sleep_for(100ms);
            if (!g_done.load()) {
                std::fprintf(stderr, "    TIMEOUT (30 s)\n");
                std::_Exit(3);
            }
        }).detach();
        system::co_spawn_static(run_case(c.fn, &rt), 0);
        rt.run();
        std::fflush(stderr);
        std::_Exit(g_failed ? 1 : 0);
    }
}

int main(int argc, char **argv) {
    if (const char *pg = std::getenv("UPQ_TEST_PG")) {
        std::istringstream in(pg);
        Server s;
        in >> s.host >> s.port >> s.user >> s.password >> s.db;
        if (!s.db.empty()) g_pg = s;
    }
    if (const char *fp = std::getenv("UPQ_TEST_FAKE_PORT")) g_fake_port = fp;
    const std::string filter = argc > 1 ? argv[1] : "";

    int passed = 0, failed = 0, skipped = 0;
    for (const auto &c : kCases) {
        if (!filter.empty() && std::string(c.name).find(filter) == std::string::npos) continue;
        if ((c.needs == 'p' && !g_pg) || (c.needs == 'f' && !g_fake_port)) {
            std::printf("SKIP %s\n", c.name);
            ++skipped;
            continue;
        }
        std::fflush(stdout);
        const pid_t pid = fork();
        if (pid == 0) return run_in_child(c);
        int status = 0;
        waitpid(pid, &status, 0);
        const bool ok = WIFEXITED(status) && WEXITSTATUS(status) == 0;
        std::printf("%s %s\n", ok ? "PASS" : "FAIL", c.name);
        if (!ok && WIFSIGNALED(status)) std::printf("    killed by signal %d\n", WTERMSIG(status));
        ok ? ++passed : ++failed;
    }
    std::printf("\n%d passed, %d failed, %d skipped\n", passed, failed, skipped);
    return failed ? 1 : 0;
}
