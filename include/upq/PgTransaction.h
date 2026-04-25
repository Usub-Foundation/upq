#ifndef PGTRANSACTION_H
#define PGTRANSACTION_H

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <cstdint>
#include <atomic>
#include <optional>
#include <vector>
#include <expected>
#include <cassert>

#include "PgPool.h"
#include "PgConnection.h"
#include "PgTypes.h"
#include "PgReflect.h"
#include "uvent/Uvent.h"

namespace usub::pg {
    enum class TxIsolationLevel : uint8_t {
        Default = 0,
        ReadCommitted,
        RepeatableRead,
        Serializable
    };

    struct PgTransactionConfig {
        TxIsolationLevel isolation = TxIsolationLevel::Default;
        bool read_only = false;
        bool deferrable = false;
        bool force_real_begin = false;
    };

    class PgTransaction {
    public:
        explicit PgTransaction(PgPool *pool, PgTransactionConfig cfg = {});

        ~PgTransaction();

        usub::uvent::task::Awaitable<bool> begin();

        usub::uvent::task::Awaitable<std::optional<PgOpError> >
        begin_errored();

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query(std::string sql, Args &&... args);

        template<class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        query_reflect(std::string sql, const Obj &obj) {
            if (!active_ || !conn_ || !conn_->connected()) {
                QueryResult bad;
                bad.ok = false;
                bad.code = PgErrorCode::InvalidFuture;
                bad.error = "transaction not active";
                bad.rows_valid = false;
                co_return bad;
            }
            co_return co_await conn_->exec_param_query_nonblocking(std::move(sql), obj);
        }

        template<class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(std::string sql, const Obj &obj) {
            co_return co_await query_reflect(std::move(sql), obj);
        }

        template<class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(std::string sql) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::vector<T>{};
            co_return co_await conn_->exec_simple_query_nonblocking<T>(std::move(sql));
        }

        template<class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(std::string sql) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::optional<T>{};
            co_return co_await conn_->exec_simple_query_one_nonblocking<T>(std::move(sql));
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await pool_->query_on(conn_, std::move(sql));

            if (is_fatal_connection_error(qr)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
                committed_ = false;
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            }

            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            try {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            } catch (const std::exception &) {
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await pool_->query_on(conn_, std::move(sql));

            if (is_fatal_connection_error(qr)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
                committed_ = false;
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            }

            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            if (qr.rows.empty())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::Unknown, "no rows", {}}
                );

            try {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            } catch (const std::exception &) {
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
        }

        template<class T, class Obj>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql, const Obj &obj) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await pool_->query_on(conn_, std::move(sql), obj);

            if (is_fatal_connection_error(qr)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
                committed_ = false;
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            }

            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            try {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            } catch (const std::exception &) {
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
        }

        template<class T, class Obj>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql, const Obj &obj) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await pool_->query_on(conn_, std::move(sql), obj);

            if (is_fatal_connection_error(qr)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
                committed_ = false;
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            }

            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            if (qr.rows.empty())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::Unknown, "no rows", {}}
                );

            try {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            } catch (const std::exception &) {
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql, Args &&... args) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await pool_->query_on(
                conn_,
                std::move(sql),
                std::forward<Args>(args)...
            );

            if (is_fatal_connection_error(qr)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
                committed_ = false;
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            }

            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});

            try {
                auto vec = usub::pg::map_all_reflect_named<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            } catch (const std::exception &) {
                auto vec = usub::pg::map_all_reflect_positional<T>(qr);
                co_return std::expected<std::vector<T>, PgOpError>{std::in_place, std::move(vec)};
            }
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql, Args &&... args) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}}
                );

            QueryResult qr = co_await pool_->query_on(
                conn_,
                std::move(sql),
                std::forward<Args>(args)...
            );

            if (is_fatal_connection_error(qr)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
                committed_ = false;
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            }

            if (!qr.ok)
                co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
            if (qr.rows.empty())
                co_return std::unexpected(
                    PgOpError{PgErrorCode::Unknown, "no rows", {}}
                );

            try {
                auto v = usub::pg::map_single_reflect_named<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            } catch (const std::exception &) {
                auto v = usub::pg::map_single_reflect_positional<T>(qr, 0);
                co_return std::expected<T, PgOpError>{std::in_place, std::move(v)};
            }
        }

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        query(Sql &&sql, Args &&... args) {
            constexpr size_t actual = detail::count_total_params<Args...>();
            const std::string_view sv(sql);
            assert(detail::count_pg_params(sv) == actual &&
                "pg: $N placeholder count does not match argument count");
            return query(std::string(sv), std::forward<Args>(args)...);
        }

        template<class Obj, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        query_reflect(Sql &&sql, const Obj &obj) {
            return query_reflect(std::string(std::string_view(sql)), obj);
        }

        template<class Obj, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(Sql &&sql, const Obj &obj) {
            return exec_reflect(std::string(std::string_view(sql)), obj);
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

        template<class T, class Obj, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(Sql &&sql, const Obj &obj) {
            return query_reflect_expected<T, Obj>(std::string(std::string_view(sql)), obj);
        }

        template<class T, class Obj, typename Sql>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(Sql &&sql, const Obj &obj) {
            return query_reflect_expected_one<T, Obj>(std::string(std::string_view(sql)), obj);
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

        usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
        stream(std::string sql, uint32_t batch_size = 1000);

        template <typename... Args>
            requires (sizeof...(Args) > 0)
        usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
        stream(std::string sql, uint32_t batch_size, Args &&... args) {
            if (!active_ || !conn_ || !conn_->connected()) {
                co_return std::unexpected(PgOpError{
                    PgErrorCode::InvalidFuture, "transaction not active", {}});
            }

            const std::string cursor_name = conn_->make_cursor_name();
            QueryResult r = co_await conn_->cursor_declare_in_tx_params(
                cursor_name, sql, std::forward<Args>(args)...);
            if (!r.ok) {
                PgOpError err{r.code, r.error, r.err_detail};
                if (is_fatal_connection_error(r)) {
                    pool_->mark_dead(conn_);
                    conn_.reset();
                    active_ = false;
                    rolled_back_ = true;
                }
                co_return std::unexpected(std::move(err));
            }

            PgRowStream s;
            s.pool_ = nullptr;
            s.conn_ = conn_;
            s.cursor_name_ = cursor_name;
            s.batch_size_ = batch_size ? batch_size : 1;
            s.owns_tx_ = false;
            s.active_ = true;
            s.exhausted_ = false;
            co_return std::expected<PgRowStream, PgOpError>{std::in_place, std::move(s)};
        }

        template <class T>
        usub::uvent::task::Awaitable<std::expected<PgTypedRowStream<T>, PgOpError> >
        stream_reflect(std::string sql, uint32_t batch_size = 1000) {
            auto s = co_await stream(std::move(sql), batch_size);
            if (!s) co_return std::unexpected(s.error());
            co_return std::expected<PgTypedRowStream<T>, PgOpError>{
                std::in_place, PgTypedRowStream<T>{std::move(*s)}};
        }

        template <class T, typename... Args>
            requires (sizeof...(Args) > 0)
        usub::uvent::task::Awaitable<std::expected<PgTypedRowStream<T>, PgOpError> >
        stream_reflect(std::string sql, uint32_t batch_size, Args &&... args) {
            auto s = co_await stream(std::move(sql), batch_size,
                                     std::forward<Args>(args)...);
            if (!s) co_return std::unexpected(s.error());
            co_return std::expected<PgTypedRowStream<T>, PgOpError>{
                std::in_place, PgTypedRowStream<T>{std::move(*s)}};
        }

        template <class Sink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to(std::string sql, Sink sink) {
            if (!active_ || !conn_ || !conn_->connected()) {
                PgCopyResult bad{};
                bad.ok = false;
                bad.code = PgErrorCode::InvalidFuture;
                bad.error = "transaction not active";
                co_return bad;
            }
            PgCopyResult r = co_await conn_->copy_to(sql, sink);
            if (!r.ok && (r.code == PgErrorCode::SocketReadFailed ||
                          r.code == PgErrorCode::ConnectionClosed)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
            }
            co_return r;
        }

        template <class LineSink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to_lines(std::string sql, LineSink sink) {
            if (!active_ || !conn_ || !conn_->connected()) {
                PgCopyResult bad{};
                bad.ok = false;
                bad.code = PgErrorCode::InvalidFuture;
                bad.error = "transaction not active";
                co_return bad;
            }
            PgCopyResult r = co_await conn_->copy_to_lines(sql, sink);
            if (!r.ok && (r.code == PgErrorCode::SocketReadFailed ||
                          r.code == PgErrorCode::ConnectionClosed)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
            }
            co_return r;
        }

        template <class Source>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from(std::string sql, Source source) {
            if (!active_ || !conn_ || !conn_->connected()) {
                PgCopyResult bad{};
                bad.ok = false;
                bad.code = PgErrorCode::InvalidFuture;
                bad.error = "transaction not active";
                co_return bad;
            }
            PgCopyResult r = co_await conn_->copy_from(sql, source);
            if (!r.ok && (r.code == PgErrorCode::SocketReadFailed ||
                          r.code == PgErrorCode::ConnectionClosed)) {
                pool_->mark_dead(conn_);
                conn_.reset();
                active_ = false;
                rolled_back_ = true;
            }
            co_return r;
        }

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from_buffer(std::string sql, const void *data, size_t len);

        usub::uvent::task::Awaitable<std::expected<std::string, PgOpError> >
        export_snapshot();

        usub::uvent::task::Awaitable<std::optional<PgOpError> >
        set_snapshot(const std::string &snapshot_id);

        usub::uvent::task::Awaitable<bool> commit();

        usub::uvent::task::Awaitable<void> rollback();

        usub::uvent::task::Awaitable<void> finish();

        usub::uvent::task::Awaitable<void> abort();

        bool is_active() const noexcept { return active_; }
        bool is_committed() const noexcept { return committed_; }
        bool is_rolled_back() const noexcept { return rolled_back_; }

        class PgSubtransaction {
        public:
            PgSubtransaction(PgTransaction &parent, std::string savepoint_name);

            ~PgSubtransaction();

            usub::uvent::task::Awaitable<bool> begin();

            usub::uvent::task::Awaitable<bool> commit();

            usub::uvent::task::Awaitable<void> rollback();

            bool is_active() const noexcept { return active_; }
            bool is_committed() const noexcept { return committed_; }
            bool is_rolled_back() const noexcept { return rolled_back_; }

            template<typename... Args>
            usub::uvent::task::Awaitable<QueryResult>
            query(std::string sql, Args &&... args) {
                co_return co_await parent_.query(std::move(sql), std::forward<Args>(args)...);
            }

            template<class Obj>
            usub::uvent::task::Awaitable<QueryResult>
            query_reflect(std::string sql, const Obj &obj) {
                co_return co_await parent_.query_reflect(std::move(sql), obj);
            }

            template<class Obj>
            usub::uvent::task::Awaitable<QueryResult>
            exec_reflect(std::string sql, const Obj &obj) {
                co_return co_await parent_.exec_reflect(std::move(sql), obj);
            }

            template<class T>
            [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
            query_reflect(std::string sql) {
                co_return co_await parent_.template query_reflect<T>(std::move(sql));
            }

            template<class T>
            [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
            query_reflect_one(std::string sql) {
                co_return co_await parent_.template query_reflect_one<T>(std::move(sql));
            }

            template<class T>
            usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
            query_reflect_expected(std::string sql) {
                co_return co_await parent_.template query_reflect_expected<T>(std::move(sql));
            }

            template<class T>
            usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
            query_reflect_expected_one(std::string sql) {
                co_return co_await parent_.template query_reflect_expected_one<T>(std::move(sql));
            }

            template<class T, class Obj>
            usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
            query_reflect_expected(std::string sql, const Obj &obj) {
                co_return co_await parent_.template query_reflect_expected<T, Obj>(std::move(sql), obj);
            }

            template<class T, class Obj>
            usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
            query_reflect_expected_one(std::string sql, const Obj &obj) {
                co_return co_await parent_.template query_reflect_expected_one<T, Obj>(std::move(sql), obj);
            }

            template<class T, typename... Args>
            usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
            query_reflect_expected(std::string sql, Args &&... args) {
                co_return co_await parent_.template query_reflect_expected<T>(
                    std::move(sql), std::forward<Args>(args)...);
            }

            template<class T, typename... Args>
            usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
            query_reflect_expected_one(std::string sql, Args &&... args) {
                co_return co_await parent_.template query_reflect_expected_one<T>(
                    std::move(sql), std::forward<Args>(args)...);
            }

            template<typename Sql, typename... Args>
                requires (std::is_convertible_v<Sql, std::string_view>
                          && !std::is_same_v<std::decay_t<Sql>, std::string>)
            usub::uvent::task::Awaitable<QueryResult>
            query(Sql &&sql, Args &&... args) {
                constexpr size_t actual = detail::count_total_params<Args...>();
                const std::string_view sv(sql);
                assert(detail::count_pg_params(sv) == actual &&
                    "pg: $N placeholder count does not match argument count");
                return query(std::string(sv), std::forward<Args>(args)...);
            }

            template<class Obj, typename Sql>
                requires (std::is_convertible_v<Sql, std::string_view>
                          && !std::is_same_v<std::decay_t<Sql>, std::string>)
            usub::uvent::task::Awaitable<QueryResult>
            query_reflect(Sql &&sql, const Obj &obj) {
                return query_reflect(std::string(std::string_view(sql)), obj);
            }

            template<class Obj, typename Sql>
                requires (std::is_convertible_v<Sql, std::string_view>
                          && !std::is_same_v<std::decay_t<Sql>, std::string>)
            usub::uvent::task::Awaitable<QueryResult>
            exec_reflect(Sql &&sql, const Obj &obj) {
                return exec_reflect(std::string(std::string_view(sql)), obj);
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

            template<class T, class Obj, typename Sql>
                requires (std::is_convertible_v<Sql, std::string_view>
                          && !std::is_same_v<std::decay_t<Sql>, std::string>)
            usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
            query_reflect_expected(Sql &&sql, const Obj &obj) {
                return query_reflect_expected<T, Obj>(std::string(std::string_view(sql)), obj);
            }

            template<class T, class Obj, typename Sql>
                requires (std::is_convertible_v<Sql, std::string_view>
                          && !std::is_same_v<std::decay_t<Sql>, std::string>)
            usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
            query_reflect_expected_one(Sql &&sql, const Obj &obj) {
                return query_reflect_expected_one<T, Obj>(std::string(std::string_view(sql)), obj);
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

        private:
            PgTransaction &parent_;
            std::string sp_name_;
            bool active_{false};
            bool committed_{false};
            bool rolled_back_{false};
        };

        PgSubtransaction make_subtx();

        std::shared_ptr<PgConnectionLibpq> connection() const { return conn_; }

    private:
        PgPool *pool_;
        PgTransactionConfig cfg_;
        std::shared_ptr<PgConnectionLibpq> conn_;

        bool active_{false};
        bool committed_{false};
        bool rolled_back_{false};

        bool emulate_readonly_autocommit_{false};

        usub::uvent::task::Awaitable<bool> send_sql_nocheck(const std::string &sql);

        static std::string build_begin_sql(const PgTransactionConfig &cfg);
    };

    template<typename... Args>
    usub::uvent::task::Awaitable<QueryResult>
    PgTransaction::query(std::string sql, Args &&... args) {
        if (!active_ || !conn_) {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::InvalidFuture;
            bad.error = "transaction not active";
            bad.rows_valid = false;
            co_return bad;
        }

        if (!conn_->connected()) {
            QueryResult bad;
            bad.ok = false;
            bad.code = PgErrorCode::ConnectionClosed;
            bad.error = "connection lost in transaction";
            bad.rows_valid = false;
            active_ = false;
            rolled_back_ = true;
            committed_ = false;

            pool_->mark_dead(conn_);
            conn_.reset();
            co_return bad;
        }

        QueryResult qr = co_await pool_->query_on(
            conn_,
            std::move(sql),
            std::forward<Args>(args)...
        );

        if (is_fatal_connection_error(qr)) {
            pool_->mark_dead(conn_);
            conn_.reset();
            active_ = false;
            rolled_back_ = true;
            committed_ = false;
        }
        co_return qr;
    }
} // namespace usub::pg

#endif // PGTRANSACTION_H
