// Throughput / CPU benchmark for the hot pool paths. Uses only the API that
// existed before the retry/flattening refactor, so the same source builds
// against both versions and the numbers are comparable.
//
//   upq_bench <host> <port> <user> <password> <db> <seconds> <workers> <fibers> <scenario>
//
// Every scenario runs `fibers` coroutines spread round-robin over the uvent
// workers for `seconds`, then prints ops/s and process CPU microseconds per op
// (user+sys from getrusage, so it includes libpq and the kernel). Run one
// scenario per process (bench/run_bench.sh): several uvent runtimes in one
// process share stale thread-locals.

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include <sys/resource.h>

#include "uvent/Uvent.h"
#include "upq/PgPool.h"
#include "upq/PgTransaction.h"
#include "upq/utils/ConnInfo.h"

using namespace usub::uvent;
using namespace std::chrono_literals;

namespace {
    struct PointRow {
        int64_t id;
        std::string name;
        double score;
    };

    struct WideRow {
        int64_t id;
        std::string a;
        std::string b;
        int32_t c;
        double d;
        bool e;
        std::optional<std::string> f;
        int64_t g;
    };

    std::atomic<bool> g_stop{false};
    std::atomic<uint64_t> g_ops{0};
    std::atomic<uint64_t> g_errors{0};
    std::atomic<int> g_live{0};

    double cpu_seconds() {
        rusage ru{};
        getrusage(RUSAGE_SELF, &ru);
        return ru.ru_utime.tv_sec + ru.ru_utime.tv_usec / 1e6 + ru.ru_stime.tv_sec + ru.ru_stime.tv_usec / 1e6;
    }

    using Scenario = task::Awaitable<bool> (*)(usub::pg::PgPool *, uint64_t);

    task::Awaitable<bool> sc_point(usub::pg::PgPool *pool, uint64_t i) {
        auto r = co_await pool->query_reflect_expected<PointRow>(
            "SELECT $1::bigint AS id, $2::text AS name, 1.5::float8 AS score",
            static_cast<int64_t>(i), std::string("bench-name-longer-than-sso"));
        co_return r.has_value() && r->size() == 1;
    }

    task::Awaitable<bool> sc_simple(usub::pg::PgPool *pool, uint64_t) {
        auto r = co_await pool->query_awaitable("SELECT 1");
        co_return r.ok;
    }

    task::Awaitable<bool> sc_no_rows(usub::pg::PgPool *pool, uint64_t i) {
        auto r = co_await pool->query_reflect_expected_one<PointRow>(
            "SELECT $1::bigint AS id, 'x'::text AS name, 1.0::float8 AS score WHERE false",
            static_cast<int64_t>(i));
        co_return !r.has_value();  // "no rows" is the expected outcome
    }

    task::Awaitable<bool> sc_tx(usub::pg::PgPool *pool, uint64_t i) {
        usub::pg::PgTransaction tx(pool);
        if (!co_await tx.begin()) co_return false;
        auto r = co_await tx.query("SELECT $1::bigint", static_cast<int64_t>(i));
        const bool ok = r.ok && co_await tx.commit();
        if (!ok) co_await tx.finish();
        co_return ok;
    }

    task::Awaitable<bool> sc_wide(usub::pg::PgPool *pool, uint64_t) {
        auto r = co_await pool->query_reflect_expected<WideRow>(
            "SELECT g AS id, 'alpha-' || g AS a, 'beta' AS b, (g % 1000)::int AS c, g * 0.5 AS d, "
            "(g % 2 = 0) AS e, CASE WHEN g % 3 = 0 THEN NULL ELSE 'f' END AS f, g * 7 AS g "
            "FROM generate_series(1, 2000) AS g");
        co_return r.has_value() && r->size() == 2000;
    }

    // Same rows, but the struct's field names do not match the columns, so the
    // named mapping fails and every call falls back to positional mapping.
    struct WidePositional {
        int64_t x0;
        std::string x1;
        std::string x2;
        int32_t x3;
        double x4;
        bool x5;
        std::optional<std::string> x6;
        int64_t x7;
    };

    task::Awaitable<bool> sc_wide_positional(usub::pg::PgPool *pool, uint64_t) {
        auto r = co_await pool->query_reflect_expected<WidePositional>(
            "SELECT g, 'alpha-' || g, 'beta', (g % 1000)::int, g * 0.5, "
            "(g % 2 = 0), CASE WHEN g % 3 = 0 THEN NULL ELSE 'f' END, g * 7 "
            "FROM generate_series(1, 200) AS g");
        co_return r.has_value() && r->size() == 200;
    }

    // A fresh session per op (handshake + auth + close): what a pool pays on
    // warm-up and on every reconnect after a dropped connection.
    std::string g_conninfo;

    task::Awaitable<bool> sc_connect(usub::pg::PgPool *, uint64_t) {
        auto conn = std::make_shared<usub::pg::PgConnectionLibpq>();
        auto err = co_await conn->connect_async(g_conninfo);
        conn->close();
        co_return !err.has_value();
    }

    task::Awaitable<void> bench_fiber(usub::pg::PgPool *pool, Scenario sc, uint64_t seed) {
        uint64_t i = seed;
        while (!g_stop.load(std::memory_order_relaxed)) {
            if (co_await sc(pool, i++))
                g_ops.fetch_add(1, std::memory_order_relaxed);
            else
                g_errors.fetch_add(1, std::memory_order_relaxed);
        }
        g_live.fetch_sub(1, std::memory_order_relaxed);
    }
}

int main(int argc, char **argv) {
    if (argc < 10) {
        std::fprintf(stderr, "usage: %s host port user password db [seconds] [workers] [fibers] [scenario]\n", argv[0]);
        return 2;
    }
    const int seconds = std::atoi(argv[6]);
    const int workers = std::atoi(argv[7]);
    const int fibers = std::atoi(argv[8]);
    const std::string only = argv[9];

    struct Named {
        const char *name;
        Scenario fn;
        int fibers_override;
    };
    const Named all[] = {
        {"point_reflect", &sc_point, 0},
        {"simple_select1", &sc_simple, 0},
        {"no_rows_one", &sc_no_rows, 0},
        {"tx_begin_query_commit", &sc_tx, 0},
        {"wide_2000_rows_named", &sc_wide, 4},
        {"wide_200_rows_positional", &sc_wide_positional, 4},
        {"connect_close", &sc_connect, 4},
    };
    if (auto ci = usub::pg::make_conninfo(argv[1], argv[2], argv[3], argv[5], argv[4], {}, {}))
        g_conninfo = *ci;

    for (const auto &s : all) {
        if (only != s.name) continue;
        const int nf = s.fibers_override ? s.fibers_override : fibers;

        usub::Uvent rt(workers);
        usub::pg::PgPool pool(argv[1], argv[2], argv[3], argv[5], argv[4], static_cast<size_t>(nf), 3);

        g_stop = false;
        g_ops = 0;
        g_errors = 0;
        g_live = nf;
        int tid = 0;
        for (int f = 0; f < nf; ++f) {
            system::co_spawn_static(bench_fiber(&pool, s.fn, static_cast<uint64_t>(f) * 1000000u), tid);
            tid = (tid + 1) % workers;
        }

        // Server-side session counter (reconnect churn), read through its own
        // one-connection pool so the measured pool is not disturbed.
        usub::pg::PgPool meta(argv[1], argv[2], argv[3], argv[5], argv[4], 1, 3);
        std::atomic<int64_t> sessions_at{-1};
        auto read_sessions = [](usub::pg::PgPool *p, std::atomic<int64_t> *out) -> task::Awaitable<void> {
            auto r = co_await p->query_awaitable(
                "SELECT sessions FROM pg_stat_database WHERE datname = current_database()");
            out->store(r.ok && !r.rows.empty() ? std::stoll(r.rows[0][0]) : -1);
        };
        auto sessions_now = [&] {
            sessions_at = -2;
            system::co_spawn_static(read_sessions(&meta, &sessions_at), 0);
            while (sessions_at.load() == -2) std::this_thread::sleep_for(1ms);
            return sessions_at.load();
        };

        // Warm-up second (pool fill, TLS, plan caches), then the measured window.
        std::thread timer([&] {
            std::this_thread::sleep_for(1s);
            const int64_t sess0 = sessions_now();
            const uint64_t ops0 = g_ops.load();
            const uint64_t err0 = g_errors.load();
            const double cpu0 = cpu_seconds();
            const auto t0 = std::chrono::steady_clock::now();
            std::this_thread::sleep_for(std::chrono::seconds(seconds));
            const double cpu1 = cpu_seconds();
            const auto t1 = std::chrono::steady_clock::now();
            const uint64_t ops = g_ops.load() - ops0;
            const uint64_t errs = g_errors.load() - err0;
            const double wall = std::chrono::duration<double>(t1 - t0).count();
            const int64_t sess1 = sessions_now();
            std::printf("%-26s ops/s=%10.0f  cpu_us/op=%8.2f  errors=%llu  new_sessions=%lld\n", s.name,
                        ops / wall, ops ? (cpu1 - cpu0) * 1e6 / static_cast<double>(ops) : 0.0,
                        static_cast<unsigned long long>(errs), static_cast<long long>(sess1 - sess0));
            std::fflush(stdout);
            g_stop = true;
            while (g_live.load() > 0) std::this_thread::sleep_for(10ms);
            rt.stop();
        });
        rt.run();
        timer.join();
    }
    return 0;
}
