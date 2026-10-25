#ifndef PGPOOL_H
#define PGPOOL_H

#include <atomic>
#include <chrono>
#include <cstdio>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "uvent/Uvent.h"
#include "uvent/sync/AsyncSemaphore.h"
#include "uvent/utils/datastructures/queue/ConcurrentQueues.h"
#include "PgConnection.h"
#include "PgReflect.h"
#include "PgRetry.h"
#include "PgRowStream.h"
#include "PgTypes.h"
#include "utils/ConnInfo.h"

#if UPQ_POOL_DEBUG
#define UPQ_POOL_DBG(fmt, ...) \
    do { std::fprintf(stderr, "[UPQ/pool] " fmt "\n", ##__VA_ARGS__); } while (0)
#else
#define UPQ_POOL_DBG(fmt, ...) \
    do { } while (0)
#endif

namespace usub::pg {
    struct HealthStats {
        std::atomic<uint64_t> checked{0};
        std::atomic<uint64_t> alive{0};
        std::atomic<uint64_t> reconnected{0};
        // Retries per RetryClass (see PgRetry.h).
        std::atomic<uint64_t> transient_retries{0};        // NotStarted
        // Awaits cut short by io_watchdog_loop() (see PgConnectionLibpq::kIoDeadlineMs).
        std::atomic<uint64_t> io_watchdog_aborts{0};
        std::atomic<uint64_t> connection_loss_retries{0};  // ConnectionLost
        std::atomic<uint64_t> deadline_retries{0};         // Deadline
        std::atomic<uint64_t> rollback_retries{0};         // RolledBack
        // Idle connections found closed by the server before reuse (dropped
        // instead of failing the next statement).
        std::atomic<uint64_t> stale_dropped{0};
    };

    inline bool is_fatal_connection_error(const QueryResult &qr) {
        if (qr.ok)
            return false;
        if (qr.code == PgErrorCode::SocketReadFailed || qr.code == PgErrorCode::ConnectionClosed)
            return true;
        return qr.error.find("another command is already in progress") != std::string::npos ||
               qr.error.find("could not receive data from server") != std::string::npos ||
               qr.error.find("server closed the connection unexpectedly") != std::string::npos;
    }

    // The NotStarted SQLSTATEs (see RetryClass::NotStarted).
    inline bool is_transient_pooler_sqlstate(std::string_view s) {
        return s == "08P01" || s == "28P01" || s == "53300" || s == "57P03";
    }

    inline bool is_transient_pooler_error(const PgErrorDetail &d) {
        return is_transient_pooler_sqlstate(d.sqlstate);
    }

    inline bool is_transient_pooler_error(const QueryResult &qr) {
        return !qr.ok && is_transient_pooler_sqlstate(qr.err_detail.sqlstate);
    }

    // The connection died under the statement and the IO watchdog was not
    // involved (kept for callers of the previous API; the pool itself uses
    // classify_retry()).
    inline bool is_connection_loss(PgErrorCode code, const PgConnectionLibpq &conn) {
        return (code == PgErrorCode::ConnectionClosed || code == PgErrorCode::SocketReadFailed) &&
               !conn.io_timed_out();
    }

    class PgPool {
    public:
        using ConnPtr = std::shared_ptr<PgConnectionLibpq>;

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

        // ---------- connections ----------

        usub::uvent::task::Awaitable<std::expected<ConnPtr, PgOpError> >
        acquire_connection();

        void release_connection(ConnPtr conn);

        usub::uvent::task::Awaitable<void>
        release_connection_async(ConnPtr conn);

        void mark_dead(ConnPtr const &conn);

        // IO watchdog: one coroutine per pool that every second walks the
        // pool's connections and cuts awaits parked longer than their IO
        // deadline via ::shutdown(2) (safe from any worker; the owner poller
        // wakes the coroutine with HUP). Started lazily by the first
        // acquire; calling it yourself is harmless (a second instance exits).
        usub::uvent::task::Awaitable<void> io_watchdog_loop();

        // ---------- statements on a connection you hold ----------
        //
        // No retries, no release: the caller owns the connection.

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_on(ConnPtr const &conn, std::string sql, Args &&... args) {
            return run_on(conn, std::move(sql), detail::AsResult{}, std::forward<Args>(args)...);
        }

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        query_on(ConnPtr const &conn, Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_on(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T>
        usub::uvent::task::Awaitable<std::vector<T> >
        query_on_reflect(ConnPtr const &conn, std::string sql) {
            return run_on(conn, std::move(sql), detail::AsRows<T>{});
        }

        template<class T>
        usub::uvent::task::Awaitable<std::optional<T> >
        query_on_reflect_one(ConnPtr const &conn, std::string sql) {
            return run_on(conn, std::move(sql), detail::AsOptionalRow<T>{});
        }

        template<class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_on_reflect(ConnPtr const &conn, std::string sql, Args &&... args) {
            return run_on(conn, std::move(sql), detail::AsRows<T>{}, std::forward<Args>(args)...);
        }

        template<class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_on_reflect_one(ConnPtr const &conn, std::string sql, Args &&... args) {
            return run_on(conn, std::move(sql), detail::AsOptionalRow<T>{}, std::forward<Args>(args)...);
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_on_reflect_expected(ConnPtr const &conn, std::string sql) {
            return run_on(conn, std::move(sql), detail::AsExpectedRows<T>{});
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_on_reflect_expected_one(ConnPtr const &conn, std::string sql) {
            return run_on(conn, std::move(sql), detail::AsExpectedOne<T>{});
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_on_reflect_expected(ConnPtr const &conn, std::string sql, Args &&... args) {
            return run_on(conn, std::move(sql), detail::AsExpectedRows<T>{}, std::forward<Args>(args)...);
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_on_reflect_expected_one(ConnPtr const &conn, std::string sql, Args &&... args) {
            return run_on(conn, std::move(sql), detail::AsExpectedOne<T>{}, std::forward<Args>(args)...);
        }

        template<class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect_on(ConnPtr const &conn, std::string sql, const Obj &obj) {
            return run_on(conn, std::move(sql), detail::AsResult{}, obj);
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::vector<T> >
        query_on_reflect(ConnPtr const &conn, Sql &&sql) {
            return query_on_reflect<T>(conn, std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::optional<T> >
        query_on_reflect_one(ConnPtr const &conn, Sql &&sql) {
            return query_on_reflect_one<T>(conn, std::string(std::string_view(sql)));
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::vector<T> >
        query_on_reflect(ConnPtr const &conn, Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_on_reflect<T>(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::optional<T> >
        query_on_reflect_one(ConnPtr const &conn, Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_on_reflect_one<T>(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_on_reflect_expected(ConnPtr const &conn, Sql &&sql) {
            return query_on_reflect_expected<T>(conn, std::string(std::string_view(sql)));
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_on_reflect_expected_one(ConnPtr const &conn, Sql &&sql) {
            return query_on_reflect_expected_one<T>(conn, std::string(std::string_view(sql)));
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_on_reflect_expected(ConnPtr const &conn, Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_on_reflect_expected<T>(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_on_reflect_expected_one(ConnPtr const &conn, Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_on_reflect_expected_one<T>(conn, std::string(sv), std::forward<Args>(args)...);
        }

        template<class Obj, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect_on(ConnPtr const &conn, Sql &&sql, const Obj &obj) {
            return exec_reflect_on(conn, std::string(std::string_view(sql)), obj);
        }

        // ---------- pooled statements ----------
        //
        // Acquire, run, release, retry by RetryClass (see QueryOpts), convert.
        // The overloads without QueryOpts use QueryOpts{}: only NotStarted
        // failures are retried.

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_awaitable(std::string sql, Args &&... args) {
            return run(QueryOpts{}, std::move(sql), detail::AsResult{}, std::forward<Args>(args)...);
        }

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query_awaitable(QueryOpts opts, std::string sql, Args &&... args) {
            return run(opts, std::move(sql), detail::AsResult{}, std::forward<Args>(args)...);
        }

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        query_awaitable(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_awaitable(std::string(sv), std::forward<Args>(args)...);
        }

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        query_awaitable(QueryOpts opts, Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_awaitable(opts, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(std::string sql) {
            return run(QueryOpts{}, std::move(sql), detail::AsRows<T>{});
        }

        template<class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(std::string sql) {
            return run(QueryOpts{}, std::move(sql), detail::AsOptionalRow<T>{});
        }

        template<class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(std::string sql, Args &&... args) {
            return run(QueryOpts{}, std::move(sql), detail::AsRows<T>{}, std::forward<Args>(args)...);
        }

        template<class T, typename... Args>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(std::string sql, Args &&... args) {
            return run(QueryOpts{}, std::move(sql), detail::AsOptionalRow<T>{}, std::forward<Args>(args)...);
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(Sql &&sql) {
            return run(QueryOpts{}, std::string(std::string_view(sql)), detail::AsRows<T>{});
        }

        template<class T, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(Sql &&sql) {
            return run(QueryOpts{}, std::string(std::string_view(sql)), detail::AsOptionalRow<T>{});
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return run(QueryOpts{}, std::string(sv), detail::AsRows<T>{}, std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return run(QueryOpts{}, std::string(sv), detail::AsOptionalRow<T>{}, std::forward<Args>(args)...);
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql) {
            return run(QueryOpts{}, std::move(sql), detail::AsExpectedRows<T>{});
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql) {
            return run(QueryOpts{}, std::move(sql), detail::AsExpectedOne<T>{});
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(QueryOpts opts, std::string sql, Args &&... args) {
            return run(opts, std::move(sql), detail::AsExpectedRows<T>{}, std::forward<Args>(args)...);
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql, Args &&... args) {
            return run(QueryOpts{}, std::move(sql), detail::AsExpectedRows<T>{}, std::forward<Args>(args)...);
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(QueryOpts opts, std::string sql, Args &&... args) {
            return run(opts, std::move(sql), detail::AsExpectedOne<T>{}, std::forward<Args>(args)...);
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql, Args &&... args) {
            return run(QueryOpts{}, std::move(sql), detail::AsExpectedOne<T>{}, std::forward<Args>(args)...);
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
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_reflect_expected<T>(std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(QueryOpts opts, Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_reflect_expected<T>(opts, std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_reflect_expected_one<T>(std::string(sv), std::forward<Args>(args)...);
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(QueryOpts opts, Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_reflect_expected_one<T>(opts, std::string(sv), std::forward<Args>(args)...);
        }

        template<class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(std::string sql, const Obj &obj) {
            return run(QueryOpts{}, std::move(sql), detail::AsResult{}, obj);
        }

        template<class Obj, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(Sql &&sql, const Obj &obj) {
            return exec_reflect(std::string(std::string_view(sql)), obj);
        }

        // ---------- cursors / COPY (one connection for the whole call) ----------

        usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
        stream(std::string sql, uint32_t batch_size = 1000);

        template <typename... Args>
            requires (sizeof...(Args) > 0)
        usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
        stream(std::string sql, uint32_t batch_size, Args &&... args);

        template <class T>
        usub::uvent::task::Awaitable<std::expected<PgTypedRowStream<T>, PgOpError> >
        stream_reflect(std::string sql, uint32_t batch_size = 1000) {
            auto s = co_await stream(std::move(sql), batch_size);
            if (!s) co_return std::unexpected(s.error());
            co_return std::expected<PgTypedRowStream<T>, PgOpError>{std::in_place, PgTypedRowStream<T>{std::move(*s)}};
        }

        template <class T, typename... Args>
            requires (sizeof...(Args) > 0)
        usub::uvent::task::Awaitable<std::expected<PgTypedRowStream<T>, PgOpError> >
        stream_reflect(std::string sql, uint32_t batch_size, Args &&... args) {
            auto s = co_await stream(std::move(sql), batch_size, std::forward<Args>(args)...);
            if (!s) co_return std::unexpected(s.error());
            co_return std::expected<PgTypedRowStream<T>, PgOpError>{std::in_place, PgTypedRowStream<T>{std::move(*s)}};
        }

        template <class Sink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to(std::string sql, Sink sink) {
            auto c = co_await acquire_connection();
            if (!c) co_return copy_error(c.error());
            PgCopyResult r = co_await (*c)->copy_to(sql, sink);
            co_await give_back_copy(*c, r);
            co_return r;
        }

        template <class LineSink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to_lines(std::string sql, LineSink sink) {
            auto c = co_await acquire_connection();
            if (!c) co_return copy_error(c.error());
            PgCopyResult r = co_await (*c)->copy_to_lines(sql, sink);
            co_await give_back_copy(*c, r);
            co_return r;
        }

        template <class Source>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from(std::string sql, Source source) {
            auto c = co_await acquire_connection();
            if (!c) co_return copy_error(c.error());
            PgCopyResult r = co_await (*c)->copy_from(sql, source);
            co_await give_back_copy(*c, r);
            co_return r;
        }

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from_buffer(std::string sql, const void *data, size_t len);

        // ---------- settings / introspection ----------

        inline std::string host() { return this->host_; }
        inline std::string port() { return this->port_; }
        inline std::string user() { return this->user_; }
        inline std::string db() { return this->db_; }

        [[deprecated("password() leaks credentials; use make_conninfo internally")]]
        inline std::string password() { return std::string("***"); }

        inline HealthStats &health_stats() { return stats_; }

        // NotStarted retry policy: total tries of one call (or one BEGIN /
        // emulated-readonly transaction statement); attempts <= 1 disables.
        // Delay before retry i (0-based) is base_ms * 3^i.
        inline void set_transient_retry(int attempts, unsigned base_ms) {
            transient_retry_attempts_ = attempts > 0 ? attempts : 1;
            transient_retry_base_ms_ = base_ms;
        }

        inline int transient_retry_attempts() const { return transient_retry_attempts_; }

        // Called for every retry the pool, its transactions and run_tx() are
        // about to make, on the worker running the statement and before the
        // backoff sleep; must be cheap and must not suspend. Install before
        // the pool is used. Unset: one line on stderr per retry.
        inline void set_retry_observer(RetryObserver observer) { retry_observer_ = std::move(observer); }

        // Reports a NotStarted retry and sleeps its backoff for 0-based
        // attempt number `attempt`. Shared by PgPool and PgTransaction.
        // false = canceled during the sleep: do not retry.
        usub::uvent::task::Awaitable<bool>
        transient_backoff(int attempt, const char *sqlstate);

        // Reports a retry (counters + observer) without sleeping; db/user are
        // filled in from the pool.
        void note_retry(RetryEvent ev);

        // Retry decision for one failed call. Counts per class across the
        // call's attempts; `attempt` is the 0-based number of the attempt that
        // failed. Returns the backoff to sleep, or nullopt to give up.
        //
        // Statement scope follows QueryOpts (only NotStarted unless the
        // caller opted in). Transaction scope (run_tx before COMMIT) retries
        // every class: nothing was committed. TransactionCommit retries a
        // rolled-back commit, and a lost / timed-out one only when idempotent.
        enum class RetryScope : uint8_t { Statement, Transaction, TransactionCommit };

        struct RetryBudget {
            explicit RetryBudget(const QueryOpts &o) : opts(o), started_ms(PgConnectionLibpq::steady_now_ms()) {}

            QueryOpts opts;
            uint64_t started_ms;
            int attempt{0};
            int connection_lost{0};
            int deadline{0};
            int rolled_back{0};
        };

        std::optional<unsigned> next_retry_delay(RetryBudget &b, RetryClass cls,
                                                 RetryScope scope = RetryScope::Statement) const;

    private:
        friend class PgTransaction;

        // Idle connection ready for a statement, or null (none idle).
        ConnPtr take_idle();

        // Gives a connection back after a statement: pooled again when it is
        // healthy and idle, closed after a connection-level failure. False =
        // it still has unread results and needs release_connection_async().
        bool give_back(ConnPtr const &conn, const QueryResult &qr, RetryClass cls);

        usub::uvent::task::Awaitable<void> give_back_copy(ConnPtr const &conn, const PgCopyResult &r);

        static PgCopyResult copy_error(const PgOpError &e);

        template<class Finish, class... Args>
        static usub::uvent::task::Awaitable<detail::finish_result_t<Finish> >
        run_on(ConnPtr const &conn, std::string sql, Finish finish, Args &&... args) {
            if (!conn || !conn->connected())
                return detail::ready(finish(detail::closed_result("connection invalid")));
            return conn->run_query(detail::make_send(std::move(sql), std::forward<Args>(args)...), std::move(finish));
        }

        // The pooled-statement coroutine behind every acquire-run-release API.
        template<class Finish, class... Args>
        usub::uvent::task::Awaitable<detail::finish_result_t<Finish> >
        run(QueryOpts opts, std::string sql, Finish finish, Args &&... args);

        void register_connection(const ConnPtr &conn);

        void ensure_watchdog_started();

        std::string host_;
        std::string port_;
        std::string user_;
        std::string db_;
        std::string password_;

        queue::concurrent::MPMCQueue<ConnPtr> idle_;

        size_t max_pool_;
        std::atomic<size_t> live_count_;

        HealthStats stats_;
        int retries_on_connection_failed_;
        int transient_retry_attempts_{3};
        unsigned transient_retry_base_ms_{200};
        RetryObserver retry_observer_;

        usub::uvent::sync::AsyncSemaphore idle_sem_{0};
        SSLConfig ssl_config_;
        TCPKeepaliveConfig keepalive_config_;

        // Every connection ever created by this pool (weak: lifetime stays
        // with the shared_ptr holders), for the IO watchdog to scan. Pruned of
        // expired entries on each watchdog pass. Guarded by conns_mtx_ because
        // acquire_connection() runs on any worker.
        std::mutex conns_mtx_;
        std::vector<std::weak_ptr<PgConnectionLibpq> > conns_;
        std::atomic<bool> watchdog_started_{false};
        std::atomic<bool> closing_{false};
    };

    template<class Finish, class... Args>
    usub::uvent::task::Awaitable<detail::finish_result_t<Finish> >
    PgPool::run(QueryOpts opts, std::string sql, Finish finish, Args &&... args) {
        RetryBudget budget(opts);
        for (;; ++budget.attempt) {
            ConnPtr conn = take_idle();
            if (!conn) {
                auto c = co_await acquire_connection();
                if (!c) {
                    QueryResult bad;
                    bad.ok = false;
                    bad.code = c.error().code;
                    bad.error = c.error().error;
                    bad.err_detail = c.error().err_detail;
                    bad.rows_valid = false;
                    co_return finish(std::move(bad));
                }
                conn = std::move(*c);
            }

            if (opts.io_deadline_ms) conn->set_io_deadline_ms(opts.io_deadline_ms);
            // Arguments are passed as lvalues: a retry must not observe
            // moved-from values.
            QueryResult qr = co_await conn->run_query(detail::make_send(&sql, args...), detail::AsResult{});
            if (opts.io_deadline_ms) conn->set_io_deadline_ms(0);

            const RetryClass cls = classify_retry(qr, conn->io_timed_out());
            if (!give_back(conn, qr, cls))
                co_await release_connection_async(std::move(conn));

            if (!qr.ok) {
                if (const auto delay = next_retry_delay(budget, cls)) {
                    note_retry(RetryEvent{cls, budget.attempt + 1, *delay, qr.code, qr.err_detail.sqlstate,
                                          qr.error, {}, {}, false});
                    if (*delay && !co_await detail::sleep_for(std::chrono::milliseconds(*delay)))
                        co_return finish(std::move(qr));  // canceled during backoff
                    continue;
                }
            }
            co_return finish(std::move(qr));
        }
    }

    template <typename... Args>
        requires (sizeof...(Args) > 0)
    usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
    PgPool::stream(std::string sql, uint32_t batch_size, Args &&... args) {
        auto c = co_await acquire_connection();
        if (!c) co_return std::unexpected(c.error());
        ConnPtr conn = std::move(*c);

        QueryResult r = co_await conn->exec_simple_query_nonblocking("BEGIN;");
        if (r.ok) {
            const std::string cursor_name = conn->make_cursor_name();
            r = co_await conn->cursor_declare_in_tx_params(cursor_name, sql, std::forward<Args>(args)...);
            if (r.ok) co_return PgRowStream::owning(this, std::move(conn), cursor_name, batch_size);
            if (!is_fatal_connection_error(r)) co_await conn->exec_simple_query_nonblocking("ROLLBACK;");
        }
        if (is_fatal_connection_error(r)) mark_dead(conn);
        else co_await release_connection_async(std::move(conn));
        co_return std::unexpected(detail::to_op_error(r));
    }
} // namespace usub::pg

#endif // PGPOOL_H
