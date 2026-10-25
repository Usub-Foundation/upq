#ifndef PGTRANSACTION_H
#define PGTRANSACTION_H

#include <cstdint>
#include <exception>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "PgConnection.h"
#include "PgPool.h"
#include "PgReflect.h"
#include "PgRetry.h"
#include "PgTypes.h"
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

    // One transaction on one pooled connection. read_only (not deferrable,
    // not force_real_begin) transactions are emulated: no BEGIN, every
    // statement autocommits on the held connection, and a statement that
    // never started (RetryClass::NotStarted) is re-run on a fresh connection.
    //
    // The object can be reused: after commit()/rollback()/finish() another
    // begin() starts a new transaction.
    class PgTransaction {
    public:
        explicit PgTransaction(PgPool *pool, PgTransactionConfig cfg = {});

        ~PgTransaction();

        usub::uvent::task::Awaitable<bool> begin();

        usub::uvent::task::Awaitable<std::optional<PgOpError> >
        begin_errored();

        // Why the last commit() returned false (connection lost, server
        // error, or the transaction was already aborted by a failed
        // statement, in which case COMMIT only rolled it back).
        [[nodiscard]] const std::optional<PgOpError> &last_commit_error() const noexcept { return last_commit_error_; }

        // ---------- statements ----------

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        query(std::string sql, Args &&... args) {
            return run(std::move(sql), detail::AsResult{}, Precheck::DropDeadConnection, std::forward<Args>(args)...);
        }

        template<class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        query_reflect(std::string sql, const Obj &obj) {
            if (!active_ || !conn_ || !conn_->connected())
                return detail::ready(not_active_result());
            return conn_->run_query(detail::make_param_send(std::move(sql), obj), detail::AsResult{});
        }

        template<class Obj>
        usub::uvent::task::Awaitable<QueryResult>
        exec_reflect(std::string sql, const Obj &obj) {
            return query_reflect(std::move(sql), obj);
        }

        template<class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
        query_reflect(std::string sql) {
            if (!active_ || !conn_ || !conn_->connected())
                return detail::ready(std::vector<T>{});
            return conn_->run_query(detail::SimpleSend<std::string>{std::move(sql)}, detail::AsRows<T>{});
        }

        template<class T>
        [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
        query_reflect_one(std::string sql) {
            if (!active_ || !conn_ || !conn_->connected())
                return detail::ready(std::optional<T>{});
            return conn_->run_query(detail::SimpleSend<std::string>{std::move(sql)}, detail::AsOptionalRow<T>{});
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql) {
            return run(std::move(sql), detail::AsExpectedRows<T>{}, Precheck::KeepState);
        }

        template<class T>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql) {
            return run(std::move(sql), detail::AsExpectedOne<T>{}, Precheck::KeepState);
        }

        template<class T, class Obj>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql, const Obj &obj) {
            return run(std::move(sql), detail::AsExpectedRows<T>{}, Precheck::KeepState, obj);
        }

        template<class T, class Obj>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql, const Obj &obj) {
            return run(std::move(sql), detail::AsExpectedOne<T>{}, Precheck::KeepState, obj);
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
        query_reflect_expected(std::string sql, Args &&... args) {
            return run(std::move(sql), detail::AsExpectedRows<T>{}, Precheck::KeepState, std::forward<Args>(args)...);
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
        query_reflect_expected_one(std::string sql, Args &&... args) {
            return run(std::move(sql), detail::AsExpectedOne<T>{}, Precheck::KeepState, std::forward<Args>(args)...);
        }

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        query(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
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
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return query_reflect_expected<T>(std::string(sv), std::forward<Args>(args)...);
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

        // ---------- cursors / COPY / snapshots ----------

        usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
        stream(std::string sql, uint32_t batch_size = 1000);

        template <typename... Args>
            requires (sizeof...(Args) > 0)
        usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
        stream(std::string sql, uint32_t batch_size, Args &&... args) {
            if (!active_ || !conn_ || !conn_->connected())
                co_return std::unexpected(not_active_error());

            const std::string cursor_name = conn_->make_cursor_name();
            QueryResult r = co_await conn_->cursor_declare_in_tx_params(cursor_name, sql, std::forward<Args>(args)...);
            if (!r.ok) {
                if (is_fatal_connection_error(r)) drop_connection();
                co_return std::unexpected(detail::to_op_error(r));
            }
            co_return PgRowStream::borrowed(conn_, cursor_name, batch_size);
        }

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
            if (!active_ || !conn_ || !conn_->connected()) co_return not_active_copy();
            PgCopyResult r = co_await conn_->copy_to(sql, sink);
            after_copy(r);
            co_return r;
        }

        template <class LineSink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to_lines(std::string sql, LineSink sink) {
            if (!active_ || !conn_ || !conn_->connected()) co_return not_active_copy();
            PgCopyResult r = co_await conn_->copy_to_lines(sql, sink);
            after_copy(r);
            co_return r;
        }

        template <class Source>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from(std::string sql, Source source) {
            if (!active_ || !conn_ || !conn_->connected()) co_return not_active_copy();
            PgCopyResult r = co_await conn_->copy_from(sql, source);
            after_copy(r);
            co_return r;
        }

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from_buffer(std::string sql, const void *data, size_t len);

        usub::uvent::task::Awaitable<std::expected<std::string, PgOpError> >
        export_snapshot();

        usub::uvent::task::Awaitable<std::optional<PgOpError> >
        set_snapshot(const std::string &snapshot_id);

        // ---------- end of transaction ----------

        usub::uvent::task::Awaitable<bool> commit();

        usub::uvent::task::Awaitable<void> rollback();

        // Rolls back when still active, then returns the connection: the
        // cleanup to call on every error path.
        usub::uvent::task::Awaitable<void> finish();

        usub::uvent::task::Awaitable<void> abort();

        bool is_active() const noexcept { return active_; }
        bool is_committed() const noexcept { return committed_; }
        bool is_rolled_back() const noexcept { return rolled_back_; }

        // Per-transaction IO deadline (see QueryOpts::io_deadline_ms): bounds
        // every socket await of every statement run on this transaction's
        // connection, 0 = the connection default (kIoDeadlineMs). Applies to
        // the current connection at once and to any connection acquired later
        // (begin / transient re-acquire); the pool resets the override when the
        // connection is released, so it never leaks into other users.
        void set_io_deadline_ms(uint64_t ms) noexcept {
            io_deadline_ms_ = ms;
            if (conn_)
                conn_->set_io_deadline_ms(ms);
        }

        [[nodiscard]] uint64_t io_deadline_ms() const noexcept { return io_deadline_ms_; }

        // SAVEPOINT scope inside the transaction; statements go to the parent.
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
                return parent_.query(std::move(sql), std::forward<Args>(args)...);
            }

            template<class Obj>
            usub::uvent::task::Awaitable<QueryResult>
            query_reflect(std::string sql, const Obj &obj) {
                return parent_.query_reflect(std::move(sql), obj);
            }

            template<class Obj>
            usub::uvent::task::Awaitable<QueryResult>
            exec_reflect(std::string sql, const Obj &obj) {
                return parent_.exec_reflect(std::move(sql), obj);
            }

            template<class T>
            [[deprecated]] usub::uvent::task::Awaitable<std::vector<T> >
            query_reflect(std::string sql) {
                return parent_.template query_reflect<T>(std::move(sql));
            }

            template<class T>
            [[deprecated]] usub::uvent::task::Awaitable<std::optional<T> >
            query_reflect_one(std::string sql) {
                return parent_.template query_reflect_one<T>(std::move(sql));
            }

            template<class T>
            usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
            query_reflect_expected(std::string sql) {
                return parent_.template query_reflect_expected<T>(std::move(sql));
            }

            template<class T>
            usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
            query_reflect_expected_one(std::string sql) {
                return parent_.template query_reflect_expected_one<T>(std::move(sql));
            }

            template<class T, class Obj>
            usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
            query_reflect_expected(std::string sql, const Obj &obj) {
                return parent_.template query_reflect_expected<T, Obj>(std::move(sql), obj);
            }

            template<class T, class Obj>
            usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
            query_reflect_expected_one(std::string sql, const Obj &obj) {
                return parent_.template query_reflect_expected_one<T, Obj>(std::move(sql), obj);
            }

            template<class T, typename... Args>
            usub::uvent::task::Awaitable<std::expected<std::vector<T>, PgOpError> >
            query_reflect_expected(std::string sql, Args &&... args) {
                return parent_.template query_reflect_expected<T>(std::move(sql), std::forward<Args>(args)...);
            }

            template<class T, typename... Args>
            usub::uvent::task::Awaitable<std::expected<T, PgOpError> >
            query_reflect_expected_one(std::string sql, Args &&... args) {
                return parent_.template query_reflect_expected_one<T>(std::move(sql), std::forward<Args>(args)...);
            }

            template<typename Sql, typename... Args>
                requires (std::is_convertible_v<Sql, std::string_view>
                          && !std::is_same_v<std::decay_t<Sql>, std::string>)
            usub::uvent::task::Awaitable<QueryResult>
            query(Sql &&sql, Args &&... args) {
                const std::string_view sv(sql);
                detail::assert_param_count<Args...>(sv);
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
                const std::string_view sv(sql);
                detail::assert_param_count<Args...>(sv);
                return query_reflect_expected<T>(std::string(sv), std::forward<Args>(args)...);
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

        private:
            // Runs a savepoint command on the parent's connection; false when
            // it failed (a connection-level failure also ends the parent).
            usub::uvent::task::Awaitable<bool> run_control(std::string sql);

            PgTransaction &parent_;
            std::string sp_name_;
            bool active_{false};
            bool committed_{false};
            bool rolled_back_{false};
        };

        PgSubtransaction make_subtx();

        std::shared_ptr<PgConnectionLibpq> connection() const { return conn_; }

    private:
        // What a statement does when the transaction's connection is already
        // gone: query() reports ConnectionClosed and ends the transaction, the
        // reflect APIs report "transaction not active" and leave state alone
        // (both as before the refactor; callers match on these).
        enum class Precheck : uint8_t { DropDeadConnection, KeepState };

        // The coroutine behind every statement: runs it on the held
        // connection; in emulated read-only mode a NotStarted failure is
        // re-run on a fresh connection; a connection-level failure ends the
        // transaction.
        template<class Finish, class... Args>
        usub::uvent::task::Awaitable<detail::finish_result_t<Finish> >
        run(std::string sql, Finish finish, Precheck pre, Args &&... args);

        static QueryResult not_active_result();

        static PgOpError not_active_error() {
            return PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}};
        }

        static PgCopyResult not_active_copy();

        // The connection is broken: close it and mark the transaction rolled back.
        void drop_connection();

        void after_copy(const PgCopyResult &r);

        // Hands the connection back to the pool when it can be done without
        // waiting; false = call release_connection_async() with it.
        bool release_now();

        // BEGIN (or nothing, when emulated) on a fresh connection, then the
        // result converted by `finish` from "no error" / the error.
        template<class Finish>
        usub::uvent::task::Awaitable<std::invoke_result_t<Finish &, std::optional<PgOpError> > >
        open(Finish finish);

        // ROLLBACK / ABORT of an active transaction, then the connection back.
        usub::uvent::task::Awaitable<void> end_with(const std::string &sql);

        // Replaces conn_ with a fresh pool connection after a NotStarted
        // failure (emulated-readonly mode only, where every statement is
        // independent). Returns false and deactivates the transaction if no
        // connection could be acquired.
        usub::uvent::task::Awaitable<bool>
        reacquire_for_transient_retry(int attempt, const char *sqlstate);

        static std::string build_begin_sql(const PgTransactionConfig &cfg);

        PgPool *pool_;
        PgTransactionConfig cfg_;
        std::string begin_sql_;
        std::shared_ptr<PgConnectionLibpq> conn_;
        std::optional<PgOpError> last_commit_error_;

        bool active_{false};
        bool committed_{false};
        bool rolled_back_{false};

        bool emulate_readonly_autocommit_{false};
        uint64_t io_deadline_ms_{0}; // 0 = connection default
    };

    template<class Finish, class... Args>
    usub::uvent::task::Awaitable<detail::finish_result_t<Finish> >
    PgTransaction::run(std::string sql, Finish finish, Precheck pre, Args &&... args) {
        for (int attempt = 0;; ++attempt) {
            if (!active_ || !conn_)
                co_return finish(not_active_result());
            if (!conn_->connected()) {
                if (pre == Precheck::KeepState) co_return finish(not_active_result());
                drop_connection();
                co_return finish(detail::closed_result("connection lost in transaction"));
            }

            // Arguments are passed as lvalues: a retry must not observe
            // moved-from values.
            QueryResult qr = co_await conn_->run_query(detail::make_send(&sql, args...), detail::AsResult{});

            if (emulate_readonly_autocommit_ && is_transient_pooler_error(qr) &&
                attempt + 1 < pool_->transient_retry_attempts()) {
                const bool reacquired = co_await reacquire_for_transient_retry(attempt, qr.err_detail.sqlstate.c_str());
                if (reacquired) continue;
                co_return finish(std::move(qr));
            }

            if (is_fatal_connection_error(qr)) drop_connection();
            co_return finish(std::move(qr));
        }
    }

    // ---------- run_tx ----------

    namespace detail {
        template<class Body>
        using tx_body_awaitable_t = std::invoke_result_t<Body &, PgTransaction &>;

        template<class A>
        struct awaitable_value;

        template<class T>
        struct awaitable_value<uvent::task::Awaitable<T> > {
            using type = T;
        };

        template<class Body>
        using tx_body_result_t = typename awaitable_value<tx_body_awaitable_t<Body> >::type;

        // begin_errored()'s error when the pool could not hand out a
        // connection at all: acquire_connection() already retried connecting,
        // so run_tx() does not start another round.
        inline constexpr std::string_view kAcquireFailed = "failed to acquire connection from pool";

        inline bool is_acquire_failure(const PgOpError &e) {
            return e.code == PgErrorCode::ConnectionClosed && e.error == kAcquireFailed;
        }

        template<class R>
        struct is_expected_op : std::false_type {};

        template<class T>
        struct is_expected_op<std::expected<T, PgOpError> > : std::true_type {};
    } // namespace detail

    // Runs `body` in a transaction and commits it; retries the WHOLE
    // transaction on a fresh connection when it failed in a way that left
    // nothing committed:
    //   - NotStarted, ConnectionLost, Deadline, RolledBack (40001 / 40P01 /
    //     55P03) during BEGIN or the body: the server rolled everything back;
    //   - RolledBack at COMMIT (serialization failure);
    //   - ConnectionLost / Deadline at COMMIT only with opts.idempotent: the
    //     commit may or may not have happened.
    // Limits: pool's NotStarted attempts with backoff, one re-run per lost
    // connection / deadline, up to attempts-1 rollback re-runs, all within
    // opts.retry_budget_ms when set. opts.io_deadline_ms applies to every
    // statement of the transaction.
    //
    // Body: `Awaitable<std::expected<T, PgOpError>>(PgTransaction &)`. It is
    // invoked and awaited in place once per attempt, so lambda captures stay
    // valid as long as run_tx() itself is awaited where it is called. It must
    // touch nothing but the database (it may run more than once) and must not
    // commit or roll back itself. Returning an error rolls back; an exception
    // rolls back and propagates.
    template<class Body>
        requires detail::is_expected_op<detail::tx_body_result_t<Body> >::value
    uvent::task::Awaitable<detail::tx_body_result_t<Body> >
    run_tx(PgPool &pool, Body body, QueryOpts opts = {}, PgTransactionConfig cfg = {}) {
        using Result = detail::tx_body_result_t<Body>;

        PgPool::RetryBudget budget(opts);
        for (;; ++budget.attempt) {
            PgTransaction tx(&pool, cfg);
            if (opts.io_deadline_ms) tx.set_io_deadline_ms(opts.io_deadline_ms);

            std::optional<PgOpError> failure = co_await tx.begin_errored();
            bool at_commit = false;
            std::optional<Result> result;

            if (!failure) {
                std::exception_ptr thrown;
                try {
                    Result r = co_await body(tx);
                    result.emplace(std::move(r));
                } catch (...) {
                    thrown = std::current_exception();
                }
                if (thrown) {
                    co_await tx.finish();
                    std::rethrow_exception(thrown);
                }

                if (*result) {
                    at_commit = true;
                    const bool committed = co_await tx.commit();
                    if (committed || tx.is_committed()) co_return std::move(*result);
                    failure = tx.last_commit_error();
                    if (!failure)  // the body ended the transaction itself
                        failure = PgOpError{PgErrorCode::InvalidFuture, "transaction not active", {}};
                } else {
                    failure = result->error();
                }
            }
            co_await tx.finish();

            const RetryClass cls = detail::is_acquire_failure(*failure) ? RetryClass::None : classify_retry(*failure);
            const auto scope = at_commit ? PgPool::RetryScope::TransactionCommit : PgPool::RetryScope::Transaction;
            if (const auto delay = pool.next_retry_delay(budget, cls, scope)) {
                pool.note_retry(RetryEvent{cls, budget.attempt + 1, *delay, failure->code, failure->err_detail.sqlstate,
                                           failure->error, {}, {}, true});
                if (*delay == 0) continue;
                const bool slept = co_await detail::sleep_for(std::chrono::milliseconds(*delay));
                if (slept) continue;
            }
            if (result && !*result) co_return std::move(*result);
            co_return std::unexpected(std::move(*failure));
        }
    }
} // namespace usub::pg

#endif // PGTRANSACTION_H
