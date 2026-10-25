#include "upq/PgPool.h"

#include <cstdlib>
#include <utility>

namespace usub::pg {
    PgPool::PgPool(std::string host,
                   std::string port,
                   std::string user,
                   std::string db,
                   std::string password,
                   size_t max_pool_size,
                   int retries_on_connection_failed, SSLConfig ssl_config, TCPKeepaliveConfig keepalive_config)
        : host_(std::move(host))
          , port_(std::move(port))
          , user_(std::move(user))
          , db_(std::move(db))
          , password_(std::move(password))
          , idle_(max_pool_size)
          , max_pool_(max_pool_size)
          , live_count_(0)
          , stats_{}
          , retries_on_connection_failed_(retries_on_connection_failed)
          , idle_sem_(0), ssl_config_(std::move(ssl_config)), keepalive_config_(keepalive_config) {
        UPQ_POOL_DBG("ctor: host=%s port=%s user=%s db=%s max_pool=%zu retries=%d",
                     host_.c_str(), port_.c_str(), user_.c_str(), db_.c_str(),
                     max_pool_, retries_on_connection_failed_);
    }

    PgPool::~PgPool() {
        // The watchdog coroutine holds a raw `this`; pools live for the whole
        // process in practice, but make a stray loop iteration exit instead
        // of touching a dead pool.
        closing_.store(true, std::memory_order_release);
    }

    void PgPool::close_all() {
        ConnPtr conn;
        while (idle_.try_dequeue(conn)) {
            if (conn) conn->close();
            conn.reset();
        }
    }

    // ---------- watchdog ----------

    void PgPool::register_connection(const ConnPtr &conn) {
        std::lock_guard<std::mutex> lk(conns_mtx_);
        conns_.push_back(conn);
    }

    void PgPool::ensure_watchdog_started() {
        if (watchdog_started_.exchange(true, std::memory_order_acq_rel))
            return;
        // Member coroutine, no lambda captures (uvent co_spawn + captured
        // lambdas is a known lifetime trap).
        usub::uvent::system::co_spawn(this->io_watchdog_loop());
    }

    usub::uvent::task::Awaitable<void> PgPool::io_watchdog_loop() {
        using namespace std::chrono_literals;

        std::vector<ConnPtr> live;
        for (;;) {
            if (!co_await detail::sleep_for(1000ms))
                co_return;  // canceled
            if (closing_.load(std::memory_order_acquire))
                co_return;

            // Snapshot under the lock, work outside it: abort_io_from_watchdog
            // takes the connection's close mutex and must not nest under ours.
            live.clear();
            {
                std::lock_guard<std::mutex> lk(conns_mtx_);
                size_t w = 0;
                for (size_t r = 0; r < conns_.size(); ++r) {
                    if (auto sp = conns_[r].lock()) {
                        live.push_back(std::move(sp));
                        conns_[w++] = std::move(conns_[r]);
                    }
                }
                conns_.resize(w);
            }

            const uint64_t now = PgConnectionLibpq::steady_now_ms();
            for (auto &c : live) {
                const uint64_t since = c->io_await_since_ms();
                if (since == 0 || now < since + c->io_deadline_ms())
                    continue;
                if (c->abort_io_from_watchdog()) {
                    stats_.io_watchdog_aborts.fetch_add(1, std::memory_order_relaxed);
                    UPQ_POOL_DBG("io watchdog: conn=%p await parked %llu ms, socket shut down",
                                 c.get(), static_cast<unsigned long long>(now - since));
                }
            }
            live.clear();
        }
    }

    // ---------- acquire / release ----------

    PgPool::ConnPtr PgPool::take_idle() {
        ConnPtr conn;
        while (idle_.try_dequeue(conn)) {
            stats_.checked.fetch_add(1, std::memory_order_relaxed);
            if (!conn) continue;
            if (!conn->connected() || !conn->is_idle()) {
                // Released connections are idle by construction (see
                // give_back); anything else is not worth draining here.
                mark_dead(conn);
                continue;
            }
            if (!conn->idle_socket_alive()) {
                stats_.stale_dropped.fetch_add(1, std::memory_order_relaxed);
                mark_dead(conn);
                continue;
            }
            stats_.alive.fetch_add(1, std::memory_order_relaxed);
            return conn;
        }
        return nullptr;
    }

    usub::uvent::task::Awaitable<std::expected<PgPool::ConnPtr, PgOpError> >
    PgPool::acquire_connection() {
        using namespace std::chrono_literals;

        ensure_watchdog_started();

        for (;;) {
            if (ConnPtr conn = take_idle())
                co_return conn;

            size_t live = live_count_.load(std::memory_order_relaxed);
            if (live >= max_pool_) {
                UPQ_POOL_DBG("acquire: no idle and at max live=%zu, waiting on idle_sem", live);
                if (!co_await detail::acquire(idle_sem_))
                    co_return std::unexpected(PgOpError{PgErrorCode::AwaitCanceled, "acquire canceled", {}});
                continue;
            }
            if (!live_count_.compare_exchange_strong(live, live + 1, std::memory_order_acq_rel,
                                                     std::memory_order_relaxed))
                continue;

            UPQ_POOL_DBG("acquire: creating new conn (live=%zu -> %zu)", live, live + 1);
            auto conn = std::make_shared<PgConnectionLibpq>();
            register_connection(conn);

            auto conninfo = make_conninfo(host_, port_, user_, db_, password_, ssl_config_, keepalive_config_);
            if (!conninfo) {
                mark_dead(conn);
                co_return std::unexpected(PgOpError{PgErrorCode::ProtocolCorrupt, "conninfo contains NUL", {}});
            }

            std::optional<std::string> last_err;
            for (int attempt = 0; attempt < retries_on_connection_failed_; ++attempt) {
                last_err = co_await conn->connect_async(conninfo.value());
                if (!last_err) co_return conn;

                UPQ_POOL_DBG("acquire: new conn=%p connect_async failed (attempt %d/%d): %s", conn.get(),
                             attempt + 1, retries_on_connection_failed_, last_err->c_str());
                if (attempt + 1 == retries_on_connection_failed_) break;
                if (!co_await detail::sleep_for(100ms)) break;  // canceled
            }

            stats_.reconnected.fetch_add(1, std::memory_order_relaxed);
            mark_dead(conn);
            std::string msg = "Connection failed after retries";
            if (last_err) msg.append(": ").append(*last_err);
            co_return std::unexpected(PgOpError{PgErrorCode::TooManyConnections, std::move(msg), {}});
        }
    }

    void PgPool::release_connection(ConnPtr conn) {
        if (!conn) return;
        conn->set_io_deadline_ms(0);

        if (!conn->connected() || !conn->is_idle()) {
            UPQ_POOL_DBG("release: conn=%p not idle or disconnected, mark_dead", conn.get());
            mark_dead(conn);
            return;
        }
        if (!idle_.try_enqueue(conn)) {
            UPQ_POOL_DBG("release: idle queue full for conn=%p, mark_dead", conn.get());
            mark_dead(conn);
            return;
        }
        idle_sem_.release();
    }

    usub::uvent::task::Awaitable<void>
    PgPool::release_connection_async(ConnPtr conn) {
        if (!conn) co_return;
        // A transaction-scoped IO deadline (PgTransaction::set_io_deadline_ms)
        // must not outlive its transaction: whoever gets this connection next
        // starts from the connection default.
        conn->set_io_deadline_ms(0);

        if (conn->connected() && !conn->is_idle()) {
            // Unread results of a statement nobody waited for: read them off
            // so the connection can be reused.
            if (co_await conn->pump_input()) conn->discard_pending_results();
        }
        release_connection(std::move(conn));
    }

    bool PgPool::give_back(ConnPtr const &conn, const QueryResult &qr, RetryClass cls) {
        const bool conn_failed = cls == RetryClass::NotStarted || cls == RetryClass::ConnectionLost ||
                                 cls == RetryClass::Deadline || is_fatal_connection_error(qr);
        if (conn_failed || !conn->connected()) {
            mark_dead(conn);
            return true;
        }
        if (!conn->is_idle()) return false;
        release_connection(conn);
        return true;
    }

    void PgPool::mark_dead(ConnPtr const &conn) {
        if (!conn) return;

        bool expected = false;
        if (!conn->dead_accounted_.compare_exchange_strong(expected, true, std::memory_order_acq_rel,
                                                           std::memory_order_relaxed)) {
            UPQ_POOL_DBG("mark_dead: conn=%p already accounted, skipping", conn.get());
            conn->close();
            return;
        }

        UPQ_POOL_DBG("mark_dead: conn=%p", conn.get());
        conn->close();
        live_count_.fetch_sub(1, std::memory_order_relaxed);
        idle_sem_.release();
    }

    // ---------- retries ----------

    std::optional<unsigned> PgPool::next_retry_delay(RetryBudget &b, RetryClass cls, RetryScope scope) const {
        const QueryOpts &o = b.opts;
        // May the failure class be re-run at all in this scope?
        bool allowed = false;
        switch (cls) {
            case RetryClass::NotStarted:
                allowed = true;
                break;
            case RetryClass::ConnectionLost:
                allowed = scope == RetryScope::Transaction || o.idempotent ||
                          (scope == RetryScope::Statement && o.retry_on_connection_loss);
                break;
            case RetryClass::Deadline:
                allowed = scope == RetryScope::Transaction || o.idempotent;
                break;
            case RetryClass::RolledBack:
                allowed = scope != RetryScope::Statement || o.idempotent;
                break;
            case RetryClass::None:
                break;
        }
        if (!allowed) return std::nullopt;

        std::optional<unsigned> delay;
        switch (cls) {
            case RetryClass::NotStarted:
                if (b.attempt + 1 < transient_retry_attempts_) {
                    unsigned ms = transient_retry_base_ms_;
                    for (int i = 0; i < b.attempt; ++i) ms *= 3;
                    delay = ms;
                }
                break;
            case RetryClass::ConnectionLost:
                if (b.connection_lost++ == 0) delay = 0u;  // a socket died; nothing to wait for
                break;
            case RetryClass::Deadline:
                if (b.deadline++ == 0) delay = 0u;
                break;
            case RetryClass::RolledBack:
                if (b.rolled_back < transient_retry_attempts_ - 1)
                    delay = 50u << b.rolled_back++;  // 50, 100, 200 ms: let the competitor finish
                break;
            case RetryClass::None:
                break;
        }
        if (delay && o.retry_budget_ms &&
            PgConnectionLibpq::steady_now_ms() + *delay >= b.started_ms + o.retry_budget_ms)
            delay.reset();
        return delay;
    }

    void PgPool::note_retry(RetryEvent ev) {
        ev.db = db_;
        ev.user = user_;
        switch (ev.cls) {
            case RetryClass::NotStarted: stats_.transient_retries.fetch_add(1, std::memory_order_relaxed); break;
            case RetryClass::ConnectionLost: stats_.connection_loss_retries.fetch_add(1, std::memory_order_relaxed); break;
            case RetryClass::Deadline: stats_.deadline_retries.fetch_add(1, std::memory_order_relaxed); break;
            case RetryClass::RolledBack: stats_.rollback_retries.fetch_add(1, std::memory_order_relaxed); break;
            case RetryClass::None: break;
        }

        if (retry_observer_) {
            retry_observer_(ev);
            return;
        }

        const std::string sqlstate(ev.sqlstate.empty() ? std::string_view("?") : ev.sqlstate);
        if (ev.cls == RetryClass::NotStarted && !ev.in_transaction) {
            std::fprintf(stderr,
                         "[UPQ/pool] transient backend error (sqlstate %s) db=%s user=%s, "
                         "retry %d/%d in %ums\n",
                         sqlstate.c_str(), db_.c_str(), user_.c_str(), ev.attempt, transient_retry_attempts_ - 1,
                         ev.delay_ms);
            return;
        }
        // libpq messages span several lines; keep the log line grep-able.
        std::string msg;
        msg.reserve(ev.message.size());
        for (const char ch : ev.message) {
            const bool space = ch == '\n' || ch == '\t' || ch == '\r' || ch == ' ';
            if (space && (msg.empty() || msg.back() == ' ')) continue;
            msg.push_back(space ? ' ' : ch);
        }
        if (!msg.empty() && msg.back() == ' ') msg.pop_back();
        std::fprintf(stderr, "[UPQ/pool] %s retry %d after %s (code %s, sqlstate %s) db=%s user=%s in %ums: %s\n",
                     ev.in_transaction ? "transaction" : "statement", ev.attempt, toString(ev.cls),
                     toString(ev.code), sqlstate.c_str(), db_.c_str(), user_.c_str(), ev.delay_ms, msg.c_str());
    }

    usub::uvent::task::Awaitable<bool>
    PgPool::transient_backoff(int attempt, const char *sqlstate) {
        unsigned delay_ms = transient_retry_base_ms_;
        for (int i = 0; i < attempt; ++i) delay_ms *= 3;
        note_retry(RetryEvent{RetryClass::NotStarted, attempt + 1, delay_ms, PgErrorCode::ServerError,
                              sqlstate ? std::string_view(sqlstate) : std::string_view{}, {}, {}, {}, false});
        const bool slept = co_await detail::sleep_for(std::chrono::milliseconds(delay_ms));
        co_return slept;
    }

    // ---------- cursors / COPY ----------

    usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
    PgPool::stream(std::string sql, uint32_t batch_size) {
        auto c = co_await acquire_connection();
        if (!c) co_return std::unexpected(c.error());
        ConnPtr conn = std::move(*c);

        QueryResult r = co_await conn->exec_simple_query_nonblocking("BEGIN;");
        if (r.ok) {
            const std::string cursor_name = conn->make_cursor_name();
            r = co_await conn->cursor_declare_in_tx(cursor_name, sql);
            if (r.ok) co_return PgRowStream::owning(this, std::move(conn), cursor_name, batch_size);
            if (!is_fatal_connection_error(r)) co_await conn->exec_simple_query_nonblocking("ROLLBACK;");
        }
        if (is_fatal_connection_error(r)) mark_dead(conn);
        else co_await release_connection_async(std::move(conn));
        co_return std::unexpected(detail::to_op_error(r));
    }

    PgCopyResult PgPool::copy_error(const PgOpError &e) {
        PgCopyResult bad{};
        bad.ok = false;
        bad.code = e.code;
        bad.error = e.error;
        bad.err_detail = e.err_detail;
        return bad;
    }

    usub::uvent::task::Awaitable<void> PgPool::give_back_copy(ConnPtr const &conn, const PgCopyResult &r) {
        if (!r.ok && (r.code == PgErrorCode::SocketReadFailed || r.code == PgErrorCode::ConnectionClosed))
            mark_dead(conn);
        else
            co_await release_connection_async(conn);
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgPool::copy_from_buffer(std::string sql, const void *data, size_t len) {
        auto c = co_await acquire_connection();
        if (!c) co_return copy_error(c.error());
        PgCopyResult r = co_await (*c)->copy_from_buffer(sql, data, len);
        co_await give_back_copy(*c, r);
        co_return r;
    }
} // namespace usub::pg
