#ifndef PGPOOL_H
#define PGPOOL_H

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <atomic>
#include <chrono>
#include <expected>
#include <cstdio>
#include <cassert>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncSemaphore.h"
#include "PgConnection.h"
#include "PgTypes.h"
#include "PgReflect.h"
#include "PgRowStream.h"
#include "utils/ConnInfo.h"
#include "uvent/utils/datastructures/queue/ConcurrentQueues.h"

#if UPQ_POOL_DEBUG
#define UPQ_POOL_DBG(fmt, ...) \
    do { std::fprintf(stderr, "[UPQ/pool] " fmt "\n", ##__VA_ARGS__); } while (0)
#endif

namespace usub::pg {
    struct HealthStats {
        std::atomic<uint64_t> checked{0};
        std::atomic<uint64_t> alive{0};
        std::atomic<uint64_t> reconnected{0};
    };

    inline bool is_fatal_connection_error(const QueryResult &qr) {
        if (qr.ok)
            return false;

        if (qr.code == PgErrorCode::SocketReadFailed ||
            qr.code == PgErrorCode::ConnectionClosed)
            return true;

        if (!qr.error.empty()) {
            if (qr.error.find("another command is already in progress") != std::string::npos)
                return true;
            if (qr.error.find("could not receive data from server") != std::string::npos)
                return true;
            if (qr.error.find("server closed the connection unexpectedly") != std::string::npos)
                return true;
        }

        return false;
    }

    class PgPool {
    public:
        PgPool(std::string host,
               std::string port,
               std::string user,
               std::string db,
               std::string password,
               size_t max_pool_size = 32,
               int retries_on_connection_failed = 20, SSLConfig ssl_config = {},
               TCPKeepaliveConfig keepalive_config = {});

        ~PgPool();

        void close_all();

        usub::uvent::task::Awaitable<std::expected<std::shared_ptr<PgConnectionLibpq>, PgOpError> >
        acquire_connection();

        void release_connection(std::shared_ptr<PgConnectionLibpq> conn);

        usub::uvent::task::Awaitable<void>
        release_connection_async(std::shared_ptr<PgConnectionLibpq> conn);

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_on(std::shared_ptr<PgConnectionLibpq> const &conn,
                 std::string sql,
                 Args &&... args);

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_awaitable(std::string sql, Args &&... args);

        template<class T>
        usub::uvent::task::Awaitable<std::vector<T> >
        query_on_reflect(std::shared_ptr<PgConnectionLibpq> const &conn,
                         std::string sql);

        template<class T>
        usub::uvent::task::Awaitable<std::optional<T> >
        query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                             std::string sql);

        template<class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(std::string sql);

        template<class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(std::string sql);

        template<class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_on_reflect_expected(std::shared_ptr<PgConnectionLibpq> const &conn,
                                  std::string sql) {
            QueryResult qr = co_await query_on(conn, std::move(sql));
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            try {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            } catch (const std::exception &e) {
#if UPQ_POOL_DEBUG
                UPQ_POOL_DBG("query_on_reflect_expected named-map FAIL: %s — fallback to positional", e.what());
#endif
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_on_reflect_expected_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                                      std::string sql) {
            QueryResult qr = co_await query_on(conn, std::move(sql));
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            if (qr.rows.empty())
                co_return std::unexpected(PgOpError{PgErrorCode::Unknown, "no rows", {}});

            try {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            } catch (const std::exception &e) {
#if UPQ_POOL_DEBUG
                UPQ_POOL_DBG("query_on_reflect_expected_one named-one FAIL: %s — fallback to positional", e.what());
#endif
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql) {
            auto c = co_await acquire_connection();
            if (!c)
                co_return std::unexpected(c.error());

            auto conn = *c;
            auto res = co_await query_on_reflect_expected<T>(conn, std::move(sql));

            if (!res)
                mark_dead(conn);
            else
                co_await release_connection_async(conn);

            co_return res;
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql) {
            auto c = co_await acquire_connection();
            if (!c)
                co_return std::unexpected(c.error());

            auto conn = *c;
            auto res = co_await query_on_reflect_expected_one<T>(conn, std::move(sql));

            if (!res)
                mark_dead(conn);
            else
                co_await release_connection_async(conn);

            co_return res;
        }

        template<class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_on_reflect(std::shared_ptr<PgConnectionLibpq> const &conn,
                         std::string sql, Args &&... args);

        template<class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                             std::string sql, Args &&... args);

        template<class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(std::string sql, Args &&... args);

        template<class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(std::string sql, Args &&... args);

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_on_reflect_expected(std::shared_ptr<PgConnectionLibpq> const &conn,
                                  std::string sql, Args &&... args) {
            QueryResult qr = co_await query_on(conn, std::move(sql), std::forward<Args>(args)...);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            try {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            } catch (const std::exception &e) {
#if UPQ_POOL_DEBUG
                UPQ_POOL_DBG("query_on_reflect_expected (param) named-map FAIL: %s — fallback to positional", e.what());
#endif
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_on_reflect_expected_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                                      std::string sql, Args &&... args) {
            QueryResult qr = co_await query_on(conn, std::move(sql), std::forward<Args>(args)...);
            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            if (qr.rows.empty())
                co_return std::unexpected(PgOpError{PgErrorCode::Unknown, "no rows", {}});

            try {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            } catch (const std::exception &e) {
#if UPQ_POOL_DEBUG
                UPQ_POOL_DBG("query_on_reflect_expected_one (param) named-one FAIL: %s — fallback to positional",
                             e.what());
#endif
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql, Args &&... args) {
            auto c = co_await acquire_connection();
            if (!c)
                co_return std::unexpected(c.error());

            auto conn = *c;
            auto res = co_await query_on_reflect_expected<T>(
                conn, std::move(sql), std::forward<Args>(args)...);

            if (!res)
                mark_dead(conn);
            else
                co_await release_connection_async(conn);

            co_return res;
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql, Args &&... args) {
            auto c = co_await acquire_connection();
            if (!c)
                co_return std::unexpected(c.error());

            auto conn = *c;
            auto res = co_await query_on_reflect_expected_one<T>(
                conn, std::move(sql), std::forward<Args>(args)...);

            if (!res)
                mark_dead(conn);
            else
                co_await release_connection_async(conn);

            co_return res;
        }

        template<class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect_on(std::shared_ptr<PgConnectionLibpq> const &conn,
                        std::string sql,
                        const Obj &obj);

        template<class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(std::string sql, const Obj &obj);

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        query_on(std::shared_ptr<PgConnectionLibpq> const &conn,
                 Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_on(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        query_awaitable(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_awaitable(std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::vector<T> >
        query_on_reflect(std::shared_ptr<PgConnectionLibpq> const &conn,
                         Sql &&sql) {
            return query_on_reflect<T>(conn, std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::optional<T> >
        query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                             Sql &&sql) {
            return query_on_reflect_one<T>(conn, std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(Sql &&sql) {
            return query_reflect<T>(std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(Sql &&sql) {
            return query_reflect_one<T>(std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_on_reflect_expected(std::shared_ptr<PgConnectionLibpq> const &conn,
                                  Sql &&sql) {
            return query_on_reflect_expected<T>(conn, std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_on_reflect_expected_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                                      Sql &&sql) {
            return query_on_reflect_expected_one<T>(conn, std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(Sql &&sql) {
            return query_reflect_expected<T>(std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(Sql &&sql) {
            return query_reflect_expected_one<T>(std::string(std::string_view(sql)));
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::vector<T> >
        query_on_reflect(std::shared_ptr<PgConnectionLibpq> const &conn,
                         Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_on_reflect<T>(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::optional<T> >
        query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                             Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_on_reflect_one<T>(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_reflect<T>(std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_reflect_one<T>(std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_on_reflect_expected(std::shared_ptr<PgConnectionLibpq> const &conn,
                                  Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_on_reflect_expected<T>(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_on_reflect_expected_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                                      Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_on_reflect_expected_one<T>(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_reflect_expected<T>(std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query_reflect_expected_one<T>(std::string(sv), std::forward<Args>(args)...);
        }

        template<class Obj, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect_on(std::shared_ptr<PgConnectionLibpq> const &conn,
                        Sql &&sql, const Obj &obj) {
            return exec_reflect_on(conn, std::string(std::string_view(sql)), obj);
        }

        template<class Obj, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(Sql &&sql, const Obj &obj) {
            return exec_reflect(std::string(std::string_view(sql)), obj);
        }

        inline std::string host() { return this->host_; }
        inline std::string port() { return this->port_; }
        inline std::string user() { return this->user_; }
        inline std::string db() { return this->db_; }

        usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
        stream(std::string sql, uint32_t batch_size = 1000);

        template <class T>
        usub::uvent::task::Awaitable<std::expected<PgTypedRowStream<T>, PgOpError> >
        stream_reflect(std::string sql, uint32_t batch_size = 1000) {
            auto s = co_await stream(std::move(sql), batch_size);
            if (!s) co_return std::unexpected(s.error());
            co_return std::expected<PgTypedRowStream<T>, PgOpError>{
                std::in_place, PgTypedRowStream<T>{std::move(*s)}};
        }

        template <class Sink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to(std::string sql, Sink sink) {
            auto c = co_await acquire_connection();
            if (!c) {
                const auto &e = c.error();
                PgCopyResult bad{};
                bad.ok = false;
                bad.code = e.code;
                bad.error = e.error;
                bad.err_detail = e.err_detail;
                co_return bad;
            }
            auto conn = *c;
            PgCopyResult r = co_await conn->copy_to(sql, sink);
            if (!r.ok && (r.code == PgErrorCode::SocketReadFailed ||
                          r.code == PgErrorCode::ConnectionClosed)) {
                mark_dead(conn);
            } else {
                co_await release_connection_async(conn);
            }
            co_return r;
        }

        template <class LineSink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to_lines(std::string sql, LineSink sink) {
            auto c = co_await acquire_connection();
            if (!c) {
                const auto &e = c.error();
                PgCopyResult bad{};
                bad.ok = false;
                bad.code = e.code;
                bad.error = e.error;
                bad.err_detail = e.err_detail;
                co_return bad;
            }
            auto conn = *c;
            PgCopyResult r = co_await conn->copy_to_lines(sql, sink);
            if (!r.ok && (r.code == PgErrorCode::SocketReadFailed ||
                          r.code == PgErrorCode::ConnectionClosed)) {
                mark_dead(conn);
            } else {
                co_await release_connection_async(conn);
            }
            co_return r;
        }

        template <class Source>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from(std::string sql, Source source) {
            auto c = co_await acquire_connection();
            if (!c) {
                const auto &e = c.error();
                PgCopyResult bad{};
                bad.ok = false;
                bad.code = e.code;
                bad.error = e.error;
                bad.err_detail = e.err_detail;
                co_return bad;
            }
            auto conn = *c;
            PgCopyResult r = co_await conn->copy_from(sql, source);
            if (!r.ok && (r.code == PgErrorCode::SocketReadFailed ||
                          r.code == PgErrorCode::ConnectionClosed)) {
                mark_dead(conn);
            } else {
                co_await release_connection_async(conn);
            }
            co_return r;
        }

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from_buffer(std::string sql, const void *data, size_t len);

        [[deprecated("password() leaks credentials; use make_conninfo internally")]]
        inline std::string password() { return std::string("***"); }

        void mark_dead(std::shared_ptr<PgConnectionLibpq> const &conn);

        inline HealthStats &health_stats() { return stats_; }

    private:
        std::string host_;
        std::string port_;
        std::string user_;
        std::string db_;
        std::string password_;

        queue::concurrent::MPMCQueue<std::shared_ptr<PgConnectionLibpq> > idle_;

        size_t max_pool_;
        std::atomic<size_t> live_count_;

        HealthStats stats_;
        int retries_on_connection_failed_;

        usub::uvent::sync::AsyncSemaphore idle_sem_{0};
        SSLConfig ssl_config_;
        TCPKeepaliveConfig keepalive_config_;
    };

    template<typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgPool::query_on(std::shared_ptr<PgConnectionLibpq> const &conn,
                     std::string sql,
                     Args &&... args) {
        if (!conn || !conn->connected()) {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::ConnectionClosed;
            bad.error = "connection invalid";
            bad.rows_valid = false;
            co_return bad;
        }

        if constexpr (sizeof...(Args) == 0) {
            QueryResult qr = co_await conn->exec_simple_query_nonblocking(sql);
            co_return qr;
        } else {
            QueryResult qr = co_await conn->exec_param_query_nonblocking(
                std::move(sql),
                std::forward<Args>(args)...
            );
            co_return qr;
        }
    }

    template<typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgPool::query_awaitable(std::string sql, Args &&... args) {
        auto c = co_await acquire_connection();
        if (!c) {
            const auto &e = c.error();
            QueryResult bad;
            bad.ok = false;
            bad.code = e.code;
            bad.error = e.error;
            bad.err_detail = e.err_detail;
            bad.rows_valid = false;
            co_return bad;
        }

        auto conn = *c;

        QueryResult qr = co_await query_on(
            conn,
            std::move(sql),
            std::forward<Args>(args)...
        );

        if (is_fatal_connection_error(qr)) {
            mark_dead(conn);
        } else {
            co_await release_connection_async(conn);
        }

        co_return qr;
    }

    template<class T>
    usub::uvent::task::Awaitable<std::vector<T> >
    PgPool::query_on_reflect(std::shared_ptr<PgConnectionLibpq> const &conn,
                             std::string sql) {
        if (!conn || !conn->connected())
            co_return std::vector<T>{};

        auto rows = co_await conn->exec_simple_query_nonblocking<T>(std::move(sql));
        co_return rows;
    }

    template<class T>
    usub::uvent::task::Awaitable<std::optional<T> >
    PgPool::query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                                 std::string sql) {
        if (!conn || !conn->connected())
            co_return std::nullopt;

        auto row = co_await conn->exec_simple_query_one_nonblocking<T>(std::move(sql));
        co_return row;
    }

    template<class T>
    usub::uvent::task::Awaitable<std::vector<T> >
    PgPool::query_reflect(std::string sql) {
        auto c = co_await acquire_connection();
        if (!c)
            co_return std::vector<T>{};

        auto conn = *c;
        auto rows = co_await query_on_reflect<T>(conn, std::move(sql));
        co_await release_connection_async(conn);
        co_return rows;
    }

    template<class T>
    usub::uvent::task::Awaitable<std::optional<T> >
    PgPool::query_reflect_one(std::string sql) {
        auto c = co_await acquire_connection();
        if (!c)
            co_return std::optional<T>{};

        auto conn = *c;
        auto row = co_await query_on_reflect_one<T>(conn, std::move(sql));
        co_await release_connection_async(conn);
        co_return row;
    }

    template<class T, typename... Args>
    usub::uvent::task::Awaitable<std::vector<T> >
    PgPool::query_on_reflect(std::shared_ptr<PgConnectionLibpq> const &conn,
                             std::string sql, Args &&... args) {
        if (!conn || !conn->connected())
            co_return std::vector<T>{};

        auto rows = co_await conn->exec_param_query_nonblocking<T>(
            std::move(sql), std::forward<Args>(args)...);
        co_return rows;
    }

    template<class T, typename... Args>
    usub::uvent::task::Awaitable<std::optional<T> >
    PgPool::query_on_reflect_one(std::shared_ptr<PgConnectionLibpq> const &conn,
                                 std::string sql, Args &&... args) {
        if (!conn || !conn->connected())
            co_return std::nullopt;

        auto row = co_await conn->exec_param_query_one_nonblocking<T>(
            std::move(sql), std::forward<Args>(args)...);
        co_return row;
    }

    template<class T, typename... Args>
    usub::uvent::task::Awaitable<std::vector<T> >
    PgPool::query_reflect(std::string sql, Args &&... args) {
        auto c = co_await acquire_connection();
        if (!c)
            co_return std::vector<T>{};

        auto conn = *c;
        auto rows = co_await query_on_reflect<T>(
            conn, std::move(sql), std::forward<Args>(args)...);
        co_await release_connection_async(conn);
        co_return rows;
    }

    template<class T, typename... Args>
    usub::uvent::task::Awaitable<std::optional<T> >
    PgPool::query_reflect_one(std::string sql, Args &&... args) {
        auto c = co_await acquire_connection();
        if (!c)
            co_return std::optional<T>{};

        auto conn = *c;
        auto row = co_await query_on_reflect_one<T>(
            conn, std::move(sql), std::forward<Args>(args)...);
        co_await release_connection_async(conn);
        co_return row;
    }

    template<class Obj>
    usub::uvent::task::Awaitable<QueryResult>
    PgPool::exec_reflect_on(std::shared_ptr<PgConnectionLibpq> const &conn,
                            std::string sql,
                            const Obj &obj) {
        if (!conn || !conn->connected()) {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::ConnectionClosed;
            bad.error = "connection invalid";
            bad.rows_valid = false;
            co_return bad;
        }

        QueryResult qr = co_await conn->exec_param_query_nonblocking(std::move(sql), obj);
        co_return qr;
    }

    template<class Obj>
    usub::uvent::task::Awaitable<QueryResult>
    PgPool::exec_reflect(std::string sql, const Obj &obj) {
        auto c = co_await acquire_connection();
        if (!c) {
            const auto &e = c.error();
            QueryResult bad;
            bad.ok = false;
            bad.code = e.code;
            bad.error = e.error;
            bad.err_detail = e.err_detail;
            bad.rows_valid = false;
            co_return bad;
        }

        auto conn = *c;
        auto qr = co_await exec_reflect_on(conn, std::move(sql), obj);

        if (is_fatal_connection_error(qr)) {
            mark_dead(conn);
        } else {
            co_await release_connection_async(conn);
        }

        co_return qr;
    }
} // namespace usub::pg

#endif // PGPOOL_H
