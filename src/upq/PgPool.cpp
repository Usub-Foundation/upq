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
#if UPQ_POOL_DEBUG
        UPQ_POOL_DBG("ctor: host=%s port=%s user=%s db=%s max_pool=%zu retries=%d",
                     host_.c_str(), port_.c_str(), user_.c_str(), db_.c_str(),
                     max_pool_, retries_on_connection_failed_);
#endif
    }

    PgPool::~PgPool() = default;

    void PgPool::close_all() {
        std::shared_ptr<PgConnectionLibpq> conn;
        while (this->idle_.try_dequeue(conn)) {
            if (conn) {
                conn->close();
            }
            conn.reset();
        }
    }

    usub::uvent::task::Awaitable<std::expected<std::shared_ptr<PgConnectionLibpq>, PgOpError> >
    PgPool::acquire_connection() {
        using namespace std::chrono_literals;

        std::shared_ptr<PgConnectionLibpq> conn;

        for (;;) {
            if (this->idle_.try_dequeue(conn)) {
                stats_.checked.fetch_add(1, std::memory_order_relaxed);

                if (!conn) {
#if UPQ_POOL_DEBUG
                    UPQ_POOL_DBG("acquire: got null conn from idle queue, dropping");
#endif
                    continue;
                }

                if (!conn->connected()) {
#if UPQ_POOL_DEBUG
                    UPQ_POOL_DBG("acquire: got bad idle conn=%p (disconnected), dropping",
                                 conn.get());
#endif
                    mark_dead(conn);
                    continue;
                }

                if (!conn->is_idle()) {
#if UPQ_POOL_DEBUG
                    UPQ_POOL_DBG("acquire: got non-idle from idle queue conn=%p, trying to drain",
                                 conn.get());
#endif

                    bool pumped = co_await conn->pump_input();
                    if (pumped) {
                        (void) conn->drain_all_results();
                    }

                    if (!conn->connected()) {
#if UPQ_POOL_DEBUG
                        UPQ_POOL_DBG("acquire: conn=%p disconnected after drain, dropping",
                                     conn.get());
#endif
                        mark_dead(conn);
                        continue;
                    }

                    if (!conn->is_idle()) {
#if UPQ_POOL_DEBUG
                        UPQ_POOL_DBG("acquire: conn=%p still non-idle after drain, dropping",
                                     conn.get());
#endif
                        mark_dead(conn);
                        continue;
                    }
#if UPQ_POOL_DEBUG
                    UPQ_POOL_DBG("acquire: conn=%p became idle after drain, reuse", conn.get());
#endif
                }

                stats_.alive.fetch_add(1, std::memory_order_relaxed);
#if UPQ_POOL_DEBUG
                UPQ_POOL_DBG("acquire: reuse idle conn=%p", conn.get());
#endif

                co_return std::expected<std::shared_ptr<PgConnectionLibpq>, PgOpError>{
                    std::in_place, std::move(conn)
                };
            }

            size_t cur_live = this->live_count_.load(std::memory_order_relaxed);
            if (cur_live < this->max_pool_) {
                if (this->live_count_.compare_exchange_strong(
                    cur_live,
                    cur_live + 1,
                    std::memory_order_acq_rel,
                    std::memory_order_relaxed)) {
#if UPQ_POOL_DEBUG
                    UPQ_POOL_DBG("acquire: creating new conn (live=%zu -> %zu)",
                                 cur_live, cur_live + 1);
#endif

                    auto newConn = std::make_shared<PgConnectionLibpq>();

                    auto conninfo = make_conninfo(host_, port_, user_, db_, password_, ssl_config_, this->keepalive_config_);
                    if (!conninfo)
                        co_return std::unexpected(PgOpError{
                            PgErrorCode::ProtocolCorrupt, "conninfo contains NUL", {}
                        });

                    bool connected = false;
                    std::optional<std::string> last_err;

                    for (int attempt = 0; attempt < retries_on_connection_failed_; ++attempt) {
                        auto err = co_await newConn->connect_async(conninfo.value());
                        if (!err.has_value()) {
                            connected = true;
                            break;
                        }

                        last_err = err;
#if UPQ_POOL_DEBUG
                        UPQ_POOL_DBG("acquire: new conn=%p connect_async failed (attempt %d/%d): %s",
                                     newConn.get(),
                                     attempt + 1,
                                     retries_on_connection_failed_,
                                     err->c_str());
#endif

                        if (attempt + 1 < retries_on_connection_failed_)
                            co_await uvent::system::this_coroutine::sleep_for(100ms);
                    }

                    if (!connected) {
                        stats_.reconnected.fetch_add(1, std::memory_order_relaxed);
                        mark_dead(newConn);

                        std::string msg = "Connection failed after retries";
                        if (last_err) {
                            msg += ": ";
                            msg += *last_err;
                        }

                        co_return std::unexpected(PgOpError{
                            PgErrorCode::TooManyConnections,
                            std::move(msg),
                            {}
                        });
                    }
#if UPQ_POOL_DEBUG
                    UPQ_POOL_DBG("acquire: new conn=%p ready", newConn.get());
#endif
                    co_return std::expected<std::shared_ptr<PgConnectionLibpq>, PgOpError>{
                        std::in_place, std::move(newConn)
                    };
                }
                continue;
            }
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("acquire: no idle and at max live=%zu, waiting on idle_sem", cur_live);
#endif
            co_await idle_sem_.acquire();
        }
    }

    void PgPool::release_connection(std::shared_ptr<PgConnectionLibpq> conn) {
        if (!conn)
            return;

        if (!conn->connected() || !conn->is_idle()) {
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("release: conn=%p not idle or disconnected, mark_dead", conn.get());
#endif
            mark_dead(conn);
            return;
        }

        if (!this->idle_.try_enqueue(conn)) {
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("release: idle queue full for conn=%p, mark_dead", conn.get());
#endif
            mark_dead(conn);
        } else {
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("release: enqueued conn=%p", conn.get());
#endif
            idle_sem_.release();
        }
    }

    usub::uvent::task::Awaitable<void>
    PgPool::release_connection_async(std::shared_ptr<PgConnectionLibpq> conn) {
        if (!conn) {
            co_return;
        }

        if (!conn->connected()) {
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("release_async: conn=%p disconnected, mark_dead", conn.get());
#endif
            mark_dead(conn);
            co_return;
        }

        bool pumped = co_await conn->pump_input();
        if (pumped) {
            (void) conn->drain_all_results();
        }

        if (!conn->connected() || !conn->is_idle()) {
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("release_async: conn=%p not idle or disconnected after drain, mark_dead",
                         conn.get());
#endif
            mark_dead(conn);
            co_return;
        }

        if (!this->idle_.try_enqueue(conn)) {
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("release_async: idle queue full for conn=%p, mark_dead", conn.get());
#endif
            mark_dead(conn);
        } else {
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("release_async: enqueued conn=%p", conn.get());
#endif
            idle_sem_.release();
        }

        co_return;
    }

    void PgPool::mark_dead(std::shared_ptr<PgConnectionLibpq> const &conn) {
        if (!conn)
            return;

        bool expected = false;
        if (!conn->dead_accounted_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel, std::memory_order_relaxed)) {
#if UPQ_POOL_DEBUG
            UPQ_POOL_DBG("mark_dead: conn=%p already accounted, skipping", conn.get());
#endif
            conn->close();
            return;
        }

#if UPQ_POOL_DEBUG
        UPQ_POOL_DBG("mark_dead: conn=%p", conn.get());
#endif
        conn->close();
        this->live_count_.fetch_sub(1, std::memory_order_relaxed);
        idle_sem_.release();
    }

    usub::uvent::task::Awaitable<std::expected<PgRowStream, PgOpError> >
    PgPool::stream(std::string sql, uint32_t batch_size) {
        auto c = co_await acquire_connection();
        if (!c) co_return std::unexpected(c.error());

        auto conn = *c;
        const std::string cursor_name = conn->make_cursor_name();

        {
            QueryResult r = co_await conn->exec_simple_query_nonblocking("BEGIN;");
            if (!r.ok) {
                PgOpError err{r.code, r.error, r.err_detail};
                if (is_fatal_connection_error(r)) mark_dead(conn);
                else co_await release_connection_async(conn);
                co_return std::unexpected(std::move(err));
            }
        }

        {
            QueryResult r = co_await conn->cursor_declare_in_tx(cursor_name, sql);
            if (!r.ok) {
                PgOpError err{r.code, r.error, r.err_detail};
                if (is_fatal_connection_error(r)) {
                    mark_dead(conn);
                } else {
                    co_await conn->exec_simple_query_nonblocking("ROLLBACK;");
                    co_await release_connection_async(conn);
                }
                co_return std::unexpected(std::move(err));
            }
        }

        PgRowStream s;
        s.pool_ = this;
        s.conn_ = std::move(conn);
        s.cursor_name_ = cursor_name;
        s.batch_size_ = batch_size ? batch_size : 1;
        s.owns_tx_ = true;
        s.active_ = true;
        s.exhausted_ = false;
        co_return std::expected<PgRowStream, PgOpError>{std::in_place, std::move(s)};
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgPool::copy_from_buffer(std::string sql, const void *data, size_t len) {
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
        PgCopyResult r = co_await conn->copy_from_buffer(sql, data, len);
        if (!r.ok && (r.code == PgErrorCode::SocketReadFailed ||
                      r.code == PgErrorCode::ConnectionClosed)) {
            mark_dead(conn);
        } else {
            co_await release_connection_async(conn);
        }
        co_return r;
    }
} // namespace usub::pg
