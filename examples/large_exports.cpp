#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "uvent/Uvent.h"
#include "upq/PgPool.h"
#include "upq/PgTransaction.h"
#include "upq/PgRowStream.h"
#include "upq/PgStream.h"

using namespace usub::uvent;
using namespace usub::pg;

struct BigRow {
    int64_t id;
    std::string name;
    std::optional<std::string> tag;
};

task::Awaitable<void> seed(PgPool& pool) {
    co_await pool.query_awaitable(R"SQL(
        CREATE TABLE IF NOT EXISTS big_table(
            id   BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            tag  TEXT
        );
    )SQL");

    auto c = co_await pool.query_awaitable("SELECT count(*)::bigint FROM big_table;");
    int64_t have = 0;
    if (c.ok && !c.rows.empty() && !c.rows[0].cols.empty()) {
        have = std::strtoll(c.rows[0].cols[0].c_str(), nullptr, 10);
    }

    if (have >= 200000) co_return;

    std::cout << "[seed] populating big_table with 200k rows via COPY...\n";

    co_await pool.query_awaitable("TRUNCATE big_table RESTART IDENTITY;");

    std::string payload;
    payload.reserve(200000 * 16);
    for (int64_t i = 1; i <= 200000; ++i) {
        payload.append("row-");
        payload.append(std::to_string(i));
        payload.push_back(',');
        if (i % 7 == 0) {
            payload.append("\\N");
        } else {
            payload.append("t");
            payload.append(std::to_string(i % 13));
        }
        payload.push_back('\n');
    }

    PgCopyResult r = co_await pool.copy_from_buffer(
        "COPY big_table(name, tag) FROM STDIN",
        payload.data(), payload.size());

    if (!r.ok) {
        std::cerr << "[seed] COPY failed: " << r.error << "\n";
    } else {
        std::cout << "[seed] inserted " << r.rows_affected << " rows\n";
    }
}

task::Awaitable<void> demo_row_stream(PgPool& pool) {
    auto sres = co_await pool.stream(
        "SELECT id, name, tag FROM big_table ORDER BY id", 5000);

    if (!sres) {
        std::cerr << "[row_stream] failed: " << sres.error().error << "\n";
        co_return;
    }
    auto& s = *sres;

    uint64_t n = 0;
    while (co_await s.next()) ++n;
    if (!s.ok()) {
        std::cerr << "[row_stream] mid-stream error: " << s.error().error << "\n";
    }
    co_await s.close();
    std::cout << "[row_stream] iterated " << n << " rows\n";
}

task::Awaitable<void> demo_typed_stream(PgPool& pool) {
    auto sres = co_await pool.stream_reflect<BigRow>(
        "SELECT id, name, tag FROM big_table WHERE tag IS NOT NULL ORDER BY id",
        5000);
    if (!sres) {
        std::cerr << "[typed_stream] failed: " << sres.error().error << "\n";
        co_return;
    }
    auto& s = *sres;

    uint64_t n = 0;
    uint64_t tag_len_sum = 0;
    while (auto r = co_await s.next()) {
        ++n;
        if (r->tag) tag_len_sum += r->tag->size();
    }
    co_await s.close();
    std::cout << "[typed_stream] rows=" << n
              << " avg_tag_len=" << (n ? tag_len_sum / n : 0) << "\n";
}

task::Awaitable<void> demo_copy_to_file(PgPool& pool, const std::string& path) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) {
        std::cerr << "[copy_to] cannot open " << path << "\n";
        co_return;
    }

    auto sink = [&out](std::string_view chunk) -> task::Awaitable<bool> {
        out.write(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        co_return out.good();
    };

    PgCopyResult r = co_await pool.copy_to(
        "COPY (SELECT id, name, tag FROM big_table) "
        "TO STDOUT WITH (FORMAT csv, HEADER)",
        sink);

    if (!r.ok) {
        std::cerr << "[copy_to] error: " << r.error << "\n";
        co_return;
    }
    out.flush();
    std::cout << "[copy_to] wrote " << path << "\n";
}

task::Awaitable<void> demo_copy_to_lines(PgPool& pool) {
    uint64_t line_count = 0;
    uint64_t bytes = 0;
    bool skipped_header = false;
    auto on_line = [&](std::string_view line) -> task::Awaitable<bool> {
        if (!skipped_header) { skipped_header = true; co_return true; }
        ++line_count;
        bytes += line.size();
        co_return true;
    };

    PgCopyResult r = co_await pool.copy_to_lines(
        "COPY (SELECT id, name FROM big_table) "
        "TO STDOUT WITH (FORMAT csv, HEADER)",
        on_line);

    std::cout << "[copy_to_lines] ok=" << r.ok
              << " counted=" << line_count
              << " body_bytes=" << bytes << "\n";
}

task::Awaitable<void> demo_copy_from_stream(PgPool& pool) {
    co_await pool.query_awaitable(R"SQL(
        CREATE TABLE IF NOT EXISTS copy_in_target(
            id   BIGINT PRIMARY KEY,
            name TEXT
        );
        TRUNCATE copy_in_target;
    )SQL");

    struct Gen {
        int64_t cur = 1;
        int64_t end = 100000;
        std::string buf;

        task::Awaitable<std::string_view> operator()() {
            if (cur > end) co_return std::string_view{};
            buf.clear();
            const int64_t stop = std::min(end, cur + 2000 - 1);
            for (; cur <= stop; ++cur) {
                buf.append(std::to_string(cur));
                buf.push_back(',');
                buf.append("bulk-");
                buf.append(std::to_string(cur));
                buf.push_back('\n');
            }
            co_return std::string_view(buf);
        }
    };

    Gen gen;
    PgCopyResult r = co_await pool.copy_from(
        "COPY copy_in_target(id, name) FROM STDIN WITH (FORMAT csv)",
        std::move(gen));

    std::cout << "[copy_from] ok=" << r.ok
              << " rows_affected=" << r.rows_affected
              << (r.ok ? "" : std::string(" err=") + r.error) << "\n";
}

task::Awaitable<void> demo_copy_from_buffer(PgPool& pool) {
    co_await pool.query_awaitable("TRUNCATE copy_in_target;");

    std::string payload;
    for (int i = 1; i <= 5000; ++i) {
        payload.append(std::to_string(i));
        payload.append(",fast-");
        payload.append(std::to_string(i));
        payload.push_back('\n');
    }

    PgCopyResult r = co_await pool.copy_from_buffer(
        "COPY copy_in_target(id, name) FROM STDIN WITH (FORMAT csv)",
        payload.data(), payload.size());

    std::cout << "[copy_from_buffer] ok=" << r.ok
              << " rows_affected=" << r.rows_affected << "\n";
}

task::Awaitable<void> demo_snapshot_workers(PgPool& pool) {
    PgTransaction leader(&pool,
        {.isolation = TxIsolationLevel::RepeatableRead, .read_only = true, .force_real_begin = true});
    if (auto err = co_await leader.begin_errored(); err) {
        std::cerr << "[snapshot] leader begin failed: " << err->error << "\n";
        co_return;
    }

    auto snap = co_await leader.export_snapshot();
    if (!snap) {
        std::cerr << "[snapshot] export failed: " << snap.error().error << "\n";
        co_await leader.rollback();
        co_return;
    }
    const std::string snap_id = *snap;
    std::cout << "[snapshot] id=" << snap_id << "\n";

    constexpr int kWorkers = 4;
    std::atomic<uint64_t> total{0};

    auto worker = [&](int id) -> task::Awaitable<void> {
        PgTransaction tx(&pool,
            {.isolation = TxIsolationLevel::RepeatableRead, .read_only = true, .force_real_begin = true});
        if (auto err = co_await tx.begin_errored(); err) {
            std::cerr << "[snapshot] worker " << id
                      << " begin failed: " << err->error << "\n";
            co_return;
        }
        if (auto err = co_await tx.set_snapshot(snap_id); err) {
            std::cerr << "[snapshot] worker " << id
                      << " set_snapshot failed: " << err->error << "\n";
            co_await tx.rollback();
            co_return;
        }

        const std::string sql =
            "SELECT id, name, tag FROM big_table WHERE id % 4 = "
            + std::to_string(id) + " ORDER BY id";
        auto sres = co_await tx.stream_reflect<BigRow>(sql, 5000);
        if (!sres) {
            std::cerr << "[snapshot] worker " << id
                      << " stream failed: " << sres.error().error << "\n";
            co_await tx.rollback();
            co_return;
        }

        uint64_t local = 0;
        while (co_await sres->next()) ++local;
        co_await sres->close();
        co_await tx.commit();

        total.fetch_add(local, std::memory_order_relaxed);
        std::cout << "[snapshot] worker " << id << " read " << local << "\n";
    };

    std::vector<task::Awaitable<void>> tasks;
    tasks.reserve(kWorkers);
    for (int i = 0; i < kWorkers; ++i) tasks.emplace_back(worker(i));
    for (auto& t : tasks) co_await t;

    co_await leader.commit();
    std::cout << "[snapshot] total=" << total.load() << "\n";
}

task::Awaitable<void> demo_pipeline(PgPool& pool) {
    namespace ps = usub::pg::stream;

    auto sres = co_await pool.stream(
        "SELECT id, name, tag FROM big_table ORDER BY id", 5000);
    if (!sres) {
        std::cerr << "[pipeline] stream open failed: "
                  << sres.error().error << "\n";
        co_return;
    }

    uint64_t tagged = 0;

    uint64_t processed = co_await(
        std::move(*sres)
        | ps::as<BigRow>
        | ps::filter([](const BigRow& r) { return r.tag.has_value(); })
        | ps::take(50000)
        | ps::for_each([&](BigRow r) {
            if (r.tag && r.tag->starts_with("t1")) ++tagged;
        })
    );

    std::cout << "[pipeline] processed=" << processed
              << " tagged_t1x=" << tagged << "\n";

    auto s2 = co_await pool.stream("SELECT id FROM big_table", 10000);
    if (s2) {
        uint64_t even = co_await(
            std::move(*s2)
            | ps::transform([](QueryResult::Row r) -> int64_t {
                if (r.cols.empty()) return 0;
                return std::strtoll(r.cols[0].c_str(), nullptr, 10);
            })
            | ps::filter([](int64_t id) { return id % 2 == 0; })
            | ps::count
        );
        std::cout << "[pipeline] even ids: " << even << "\n";
    }

    auto s3 = co_await pool.stream_reflect<BigRow>(
        "SELECT id, name, tag FROM big_table", 10000);
    if (s3) {
        int64_t sum = co_await(
            std::move(*s3)
            | ps::reduce(int64_t{0}, [](int64_t acc, BigRow r) {
                return acc + r.id;
            })
        );
        std::cout << "[pipeline] sum(id) = " << sum << "\n";
    }
}

task::Awaitable<void> demo_stream_with_params(PgPool& pool) {
    const int64_t lower = 100000;
    const int64_t upper = 100100;

    auto sres = co_await pool.stream_reflect<BigRow>(
        "SELECT id, name, tag FROM big_table "
        "WHERE id BETWEEN $1 AND $2 ORDER BY id",
        1000, lower, upper);

    if (!sres) {
        std::cerr << "[params] stream open failed: "
                  << sres.error().error << "\n";
        co_return;
    }

    uint64_t got = 0;
    int64_t sum_id = 0;
    while (auto row = co_await sres->next()) {
        ++got;
        sum_id += row->id;
    }
    co_await sres->close();

    std::cout << "[params] rows in [" << lower << "," << upper << "]: "
              << got << " sum_id=" << sum_id << "\n";

    auto sres2 = co_await pool.stream(
        "SELECT id FROM big_table WHERE id % $1 = $2 LIMIT $3",
        500, int64_t{7}, int64_t{0}, int64_t{20});

    if (!sres2) {
        std::cerr << "[params] second stream open failed: "
                  << sres2.error().error << "\n";
        co_return;
    }

    uint64_t mult7 = 0;
    while (auto row = co_await sres2->next()) ++mult7;
    co_await sres2->close();

    std::cout << "[params] multiples-of-7 sample: " << mult7 << "\n";

    namespace ps = usub::pg::stream;

    auto s3 = co_await pool.stream_reflect<BigRow>(
        "SELECT id, name, tag FROM big_table "
        "WHERE id > $1 AND tag IS NOT NULL ORDER BY id",
        2000, int64_t{50000});

    if (!s3) {
        std::cerr << "[params] pipeline open failed: "
                  << s3.error().error << "\n";
        co_return;
    }

    int64_t pipe_sum = co_await(
        std::move(*s3)
        | ps::filter([](const BigRow& r) { return r.id % 2 == 0; })
        | ps::take(1000)
        | ps::reduce(int64_t{0}, [](int64_t acc, BigRow r) {
            return acc + r.id;
        })
    );

    std::cout << "[params] pipeline sum(even, id>50000, tagged, first 1000): "
              << pipe_sum << "\n";
}

task::Awaitable<void> run_all(PgPool& pool, usub::Uvent& uvent) {
    co_await seed(pool);
    co_await demo_row_stream(pool);
    co_await demo_typed_stream(pool);
    co_await demo_copy_to_file(pool, "/tmp/upq_export.csv");
    co_await demo_copy_to_lines(pool);
    co_await demo_copy_from_stream(pool);
    co_await demo_copy_from_buffer(pool);
    co_await demo_snapshot_workers(pool);
    co_await demo_pipeline(pool);
    co_await demo_stream_with_params(pool);
    pool.close_all();
    uvent.stop();
    co_return;
}

int main() {
    settings::timeout_duration_ms = 30000;
    usub::Uvent uvent(1);

    PgPool pool(
        "localhost",
        "12432",
        "postgres",
        "postgres",
        "password",
        16
    );

    system::co_spawn(run_all(pool, uvent));
    uvent.run();
    return 0;
}
