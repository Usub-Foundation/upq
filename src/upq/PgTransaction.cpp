#include "upq/PgTransaction.h"

#include <atomic>
#include <cstdio>

namespace usub::pg {
    namespace {
        const std::string kCommit = "COMMIT";
        const std::string kRollback = "ROLLBACK";
        const std::string kAbort = "ABORT";
    }

    std::string PgTransaction::build_begin_sql(const PgTransactionConfig &cfg) {
        const bool any_opts = cfg.isolation != TxIsolationLevel::Default || cfg.read_only || cfg.deferrable;
        if (!any_opts) return "BEGIN";

        std::string out = "BEGIN";
        switch (cfg.isolation) {
            case TxIsolationLevel::ReadCommitted: out += " ISOLATION LEVEL READ COMMITTED"; break;
            case TxIsolationLevel::RepeatableRead: out += " ISOLATION LEVEL REPEATABLE READ"; break;
            case TxIsolationLevel::Serializable: out += " ISOLATION LEVEL SERIALIZABLE"; break;
            case TxIsolationLevel::Default: break;
        }
        out += cfg.read_only ? " READ ONLY" : " READ WRITE";
        if (cfg.deferrable) out += " DEFERRABLE";
        return out;
    }

    PgTransaction::PgTransaction(PgPool *pool, PgTransactionConfig cfg)
        : pool_(pool), cfg_(cfg), begin_sql_(build_begin_sql(cfg)) {
        emulate_readonly_autocommit_ = cfg_.read_only && !cfg_.deferrable && !cfg_.force_real_begin;
    }

    PgTransaction::~PgTransaction() {
        if (!conn_) return;
        if (active_ && !committed_ && !rolled_back_) {
            std::fprintf(stderr,
                         "[UPQ/tx] WARNING: PgTransaction destroyed while active; "
                         "connection will be closed to force implicit rollback. "
                         "Always call commit()/rollback()/finish() explicitly.\n");
        }
        pool_->mark_dead(conn_);
        conn_.reset();
    }

    // ---------- begin ----------

    template<class Finish>
    usub::uvent::task::Awaitable<std::invoke_result_t<Finish &, std::optional<PgOpError> > >
    PgTransaction::open(Finish finish) {
        if (active_) co_return finish(std::nullopt);

        for (int attempt = 0;; ++attempt) {
            conn_ = pool_->take_idle();
            if (!conn_) {
                auto c = co_await pool_->acquire_connection();
                if (!c) co_return finish(PgOpError{PgErrorCode::ConnectionClosed, std::string(detail::kAcquireFailed), {}});
                conn_ = std::move(*c);
            }
            if (!conn_->connected()) {
                pool_->mark_dead(conn_);
                conn_.reset();
                co_return finish(PgOpError{PgErrorCode::ConnectionClosed, "connection not OK", {}});
            }
            if (io_deadline_ms_) conn_->set_io_deadline_ms(io_deadline_ms_);

            if (!emulate_readonly_autocommit_) {
                const QueryResult r = co_await conn_->run_query(detail::SimpleSend<const std::string *>{&begin_sql_},
                                                                detail::AsResult{});
                if (!r.ok) {
                    pool_->mark_dead(conn_);
                    conn_.reset();
                    // BEGIN never reached a backend (pooler could not hand out
                    // a server connection): safe to retry on a fresh one.
                    if (is_transient_pooler_error(r) && attempt + 1 < pool_->transient_retry_attempts()) {
                        const bool slept = co_await pool_->transient_backoff(attempt, r.err_detail.sqlstate.c_str());
                        if (slept) continue;
                    }
                    active_ = committed_ = rolled_back_ = false;
                    co_return finish(detail::to_op_error(r));
                }
            }

            active_ = true;
            committed_ = rolled_back_ = false;
            last_commit_error_.reset();
            co_return finish(std::nullopt);
        }
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::begin() {
        return open([](std::optional<PgOpError> err) { return !err.has_value(); });
    }

    usub::uvent::task::Awaitable<std::optional<PgOpError> > PgTransaction::begin_errored() {
        return open([](std::optional<PgOpError> err) { return err; });
    }

    // ---------- end ----------

    bool PgTransaction::release_now() {
        if (!conn_) return true;
        if (conn_->connected() && !conn_->is_idle()) return false;
        pool_->release_connection(std::move(conn_));
        conn_.reset();
        return true;
    }

    void PgTransaction::drop_connection() {
        if (conn_) pool_->mark_dead(conn_);
        conn_.reset();
        active_ = false;
        rolled_back_ = true;
        committed_ = false;
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::commit() {
        if (!active_) co_return false;

        last_commit_error_.reset();
        if (!conn_ || !conn_->connected()) {
            last_commit_error_ = PgOpError{PgErrorCode::ConnectionClosed, "connection lost in transaction", {}};
            drop_connection();
            co_return false;
        }

        if (emulate_readonly_autocommit_) {
            committed_ = true;
            rolled_back_ = active_ = false;
            if (!release_now()) co_await pool_->release_connection_async(std::move(conn_));
            co_return true;
        }

        // A statement already failed inside this transaction: COMMIT would
        // only answer "ROLLBACK", which libpq reports as success. Say so.
        if (PQtransactionStatus(conn_->raw_conn()) == PQTRANS_INERROR) {
            last_commit_error_ = PgOpError{PgErrorCode::ServerError,
                                           "transaction is aborted (a statement failed); COMMIT rolled it back",
                                           PgErrorDetail{"25P02", "current transaction is aborted", {}, {},
                                                         PgSqlStateClass::TransactionState}};
            co_await end_with(kRollback);
            co_return false;
        }

        const QueryResult r = co_await conn_->run_query(detail::SimpleSend<const std::string *>{&kCommit},
                                                        detail::AsResult{});
        active_ = false;
        committed_ = r.ok;
        rolled_back_ = !r.ok;
        if (!r.ok) last_commit_error_ = detail::to_op_error(r);

        if (is_fatal_connection_error(r)) {
            pool_->mark_dead(conn_);
            conn_.reset();
        } else if (!release_now()) {
            co_await pool_->release_connection_async(std::move(conn_));
        }
        co_return r.ok;
    }

    usub::uvent::task::Awaitable<void> PgTransaction::end_with(const std::string &sql) {
        if (!emulate_readonly_autocommit_ && conn_ && conn_->connected()) {
            const QueryResult r = co_await conn_->run_query(detail::SimpleSend<const std::string *>{&sql},
                                                            detail::AsResult{});
            if (is_fatal_connection_error(r)) {
                pool_->mark_dead(conn_);
                conn_.reset();
            }
        }
        committed_ = false;
        rolled_back_ = true;
        active_ = false;
        if (!release_now()) co_await pool_->release_connection_async(std::move(conn_));
    }

    usub::uvent::task::Awaitable<void> PgTransaction::rollback() {
        if (!active_) return detail::ready_void();
        return end_with(kRollback);
    }

    usub::uvent::task::Awaitable<void> PgTransaction::abort() {
        if (!active_) return detail::ready_void();
        return end_with(kAbort);
    }

    usub::uvent::task::Awaitable<void> PgTransaction::finish() {
        if (active_) return end_with(kRollback);
        if (!conn_) return detail::ready_void();
        return pool_->release_connection_async(std::move(conn_));
    }

    // ---------- statements ----------

    QueryResult PgTransaction::not_active_result() {
        QueryResult bad;
        bad.ok = false;
        bad.code = PgErrorCode::InvalidFuture;
        bad.error = "transaction not active";
        bad.rows_valid = false;
        return bad;
    }

    PgCopyResult PgTransaction::not_active_copy() {
        PgCopyResult bad{};
        bad.ok = false;
        bad.code = PgErrorCode::InvalidFuture;
        bad.error = "transaction not active";
        return bad;
    }

    void PgTransaction::after_copy(const PgCopyResult &r) {
        if (!r.ok && (r.code == PgErrorCode::SocketReadFailed || r.code == PgErrorCode::ConnectionClosed))
            drop_connection();
    }

    usub::uvent::task::Awaitable<bool>
    PgTransaction::reacquire_for_transient_retry(int attempt, const char *sqlstate) {
        if (conn_) pool_->mark_dead(conn_);
        conn_.reset();

        const bool slept = co_await pool_->transient_backoff(attempt, sqlstate);
        if (!slept) co_return false;  // canceled: the caller reports the original error

        conn_ = pool_->take_idle();
        if (!conn_) {
            auto c = co_await pool_->acquire_connection();
            if (c) conn_ = std::move(*c);
        }
        if (!conn_ || !conn_->connected()) {
            drop_connection();
            co_return false;
        }
        if (io_deadline_ms_) conn_->set_io_deadline_ms(io_deadline_ms_);
        co_return true;
    }

    usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
    PgTransaction::stream(std::string sql, uint32_t batch_size) {
        if (!active_ || !conn_ || !conn_->connected())
            co_return std::unexpected(not_active_error());

        const std::string cursor_name = conn_->make_cursor_name();
        const QueryResult r = co_await conn_->cursor_declare_in_tx(cursor_name, sql);
        if (!r.ok) {
            if (is_fatal_connection_error(r)) drop_connection();
            co_return std::unexpected(detail::to_op_error(r));
        }
        co_return PgRowStream::borrowed(conn_, cursor_name, batch_size);
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgTransaction::copy_from_buffer(std::string sql, const void *data, size_t len) {
        if (!active_ || !conn_ || !conn_->connected()) co_return not_active_copy();
        PgCopyResult r = co_await conn_->copy_from_buffer(sql, data, len);
        after_copy(r);
        co_return r;
    }

    usub::uvent::task::Awaitable<std::expected<std::string, PgOpError> >
    PgTransaction::export_snapshot() {
        if (!active_ || !conn_ || !conn_->connected())
            return detail::ready(std::expected<std::string, PgOpError>{std::unexpected(not_active_error())});
        return conn_->export_snapshot();
    }

    usub::uvent::task::Awaitable<std::optional<PgOpError> >
    PgTransaction::set_snapshot(const std::string &snapshot_id) {
        if (!active_ || !conn_ || !conn_->connected())
            return detail::ready(std::make_optional(not_active_error()));
        return conn_->set_snapshot(snapshot_id);
    }

    // ---------- savepoints ----------

    namespace {
        std::atomic<uint64_t> g_subtx_id{0};
    }

    PgTransaction::PgSubtransaction PgTransaction::make_subtx() {
        const uint64_t id = g_subtx_id.fetch_add(1, std::memory_order_relaxed);
        return PgSubtransaction(*this, "uv_sp_" + std::to_string(id));
    }

    PgTransaction::PgSubtransaction::PgSubtransaction(PgTransaction &parent, std::string savepoint_name)
        : parent_(parent), sp_name_(std::move(savepoint_name)) {
    }

    PgTransaction::PgSubtransaction::~PgSubtransaction() = default;

    usub::uvent::task::Awaitable<bool> PgTransaction::PgSubtransaction::run_control(std::string sql) {
        const QueryResult r = co_await parent_.conn_->run_query(detail::SimpleSend<std::string>{std::move(sql)},
                                                                detail::AsResult{});
        if (!r.ok && is_fatal_connection_error(r)) parent_.drop_connection();
        co_return r.ok;
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::PgSubtransaction::begin() {
        if (!parent_.active_ || !parent_.conn_ || !parent_.conn_->connected() ||
            parent_.emulate_readonly_autocommit_)
            co_return false;
        if (!co_await run_control("SAVEPOINT " + sp_name_)) co_return false;
        active_ = true;
        committed_ = rolled_back_ = false;
        co_return true;
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::PgSubtransaction::commit() {
        if (!active_) co_return false;
        if (!parent_.conn_ || !parent_.conn_->connected()) {
            active_ = committed_ = false;
            rolled_back_ = true;
            co_return false;
        }
        if (parent_.emulate_readonly_autocommit_) co_return false;

        const bool ok = co_await run_control("RELEASE SAVEPOINT " + sp_name_);
        active_ = false;
        committed_ = ok;
        rolled_back_ = !ok;
        co_return ok;
    }

    usub::uvent::task::Awaitable<void> PgTransaction::PgSubtransaction::rollback() {
        if (!active_) co_return;
        if (!parent_.emulate_readonly_autocommit_ && parent_.conn_ && parent_.conn_->connected())
            co_await run_control("ROLLBACK TO SAVEPOINT " + sp_name_);
        active_ = committed_ = false;
        rolled_back_ = true;
    }
} // namespace usub::pg
