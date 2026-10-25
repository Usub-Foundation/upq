#ifndef PGNOTIFICATIONMULTIPLEXER_H
#define PGNOTIFICATIONMULTIPLEXER_H

#include <libpq-fe.h>
#include <sys/socket.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "PgConnection.h"
#include "PgTypes.h"
#include "utils/ConnInfo.h"
#include "utils/UventCompat.h"
#include "uvent/Uvent.h"
#include "uvent/sync/AsyncEvent.h"

namespace usub::pg {
    struct IPgNotifyHandler {
        virtual usub::uvent::task::Awaitable<void> operator()(std::string channel,
                                                              std::string payload,
                                                              int backend_pid) = 0;

        virtual ~IPgNotifyHandler() = default;
    };

    // LISTEN/NOTIFY fan-out over one dedicated connection.
    //
    // Who owns the connection: before run(), add_handler() sends its LISTEN
    // itself (such calls must not overlap). From run() on, the run loop is
    // the only reader of the socket; a subscription change is queued and
    // sent by whoever finds the connection free, and the run loop, woken by
    // the reply, completes it. Every libpq call on the connection happens
    // under mtx_ in a short section without suspension, so callers on other
    // workers are safe.
    //
    // Nothing that arrives is dropped. A notification goes to each matching
    // handler in a detached coroutine of its own, started in arrival order;
    // per channel at most channel_queue_capacity handler runs are in flight,
    // and further notifications wait in an unbounded FIFO until a run
    // finishes (capacity 1 = strictly one at a time).
    class PgNotificationMultiplexer {
    public:
        struct Config {
            size_t channel_queue_capacity;          // handler runs in flight per channel, 0 = no limit
            size_t pending_after_disconnect_capacity;  // unused: nothing is held back any more
            uint64_t reconnect_backoff_us;
            uint32_t max_recursive_depth;           // unused: handlers never run nested
            uint32_t rate_limit_per_sec;            // unused: nothing is dropped any more

            constexpr Config(size_t channel_queue_capacity_ = 256,
                             size_t pending_after_disconnect_capacity_ = 1024,
                             uint64_t reconnect_backoff_us_ = 100000,
                             uint32_t max_recursive_depth_ = 4,
                             uint32_t rate_limit_per_sec_ = 1000) noexcept
                : channel_queue_capacity(channel_queue_capacity_),
                  pending_after_disconnect_capacity(pending_after_disconnect_capacity_),
                  reconnect_backoff_us(reconnect_backoff_us_),
                  max_recursive_depth(max_recursive_depth_),
                  rate_limit_per_sec(rate_limit_per_sec_) {
            }
        };

        struct HandlerHandle {
            uint64_t id;
            std::string channel;
            bool wildcard;
        };

        // The dropped_* counters stay 0 (kept for source compatibility).
        struct Stats {
            uint64_t dropped_overflow = 0;
            uint64_t dropped_recursive = 0;
            uint64_t dropped_rate_limited = 0;
            uint64_t waiting = 0;  // notifications queued behind the per-channel limit right now
        };

        PgNotificationMultiplexer(std::shared_ptr<PgConnectionLibpq> conn, std::string host,
                                  std::string port, std::string user, std::string db,
                                  std::string password, Config cfg = {}, SSLConfig ssl_config = {},
                                  TCPKeepaliveConfig keepalive_config = {})
            : conn_(std::move(conn)),
              host_(std::move(host)),
              port_(std::move(port)),
              user_(std::move(user)),
              db_(std::move(db)),
              password_(std::move(password)),
              ssl_config_(ssl_config),
              keepalive_config_(keepalive_config),
              cfg_(cfg) {
        }

        // Subscribes `handler` to `channel`; "a.b.*" is a wildcard over the
        // channels LISTENed to anyway (no LISTEN of its own). nullopt = the
        // LISTEN failed and nothing stays registered.
        usub::uvent::task::Awaitable<std::optional<HandlerHandle> >
        add_handler(std::string channel, std::shared_ptr<IPgNotifyHandler> handler) {
            const uint64_t hid = next_handler_id_.fetch_add(1, std::memory_order_relaxed);

            if (is_wildcard(channel)) {
                std::lock_guard<std::mutex> lk(mtx_);
                wildcard_[channel].emplace_back(hid, std::move(handler));
                co_return HandlerHandle{hid, std::move(channel), true};
            }

            bool first = false;
            std::shared_ptr<Command> cmd;
            std::vector<std::shared_ptr<Command> > completed;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                auto &handlers = exact_[channel];
                first = handlers.empty();
                handlers.emplace_back(hid, std::move(handler));
                channel_state_locked(channel);
                if (first && running_) {
                    cmd = std::make_shared<Command>(channel, true);
                    commands_.push_back(cmd);
                    start_next_command_locked(completed);
                } else if (first) {
                    ++direct_busy_;
                }
            }
            finish_commands(completed);
            if (!first) co_return HandlerHandle{hid, std::move(channel), false};

            bool ok = false;
            if (cmd) {
                co_await cmd->finished.wait();
                std::lock_guard<std::mutex> lk(mtx_);
                ok = cmd->done && cmd->ok;
            } else {
                ok = co_await listen_direct(channel);
                std::lock_guard<std::mutex> lk(mtx_);
                --direct_busy_;
            }
            if (!ok) {
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    erase_exact_locked(channel, hid, completed);
                }
                finish_commands(completed);
                co_return std::nullopt;
            }
            co_return HandlerHandle{hid, std::move(channel), false};
        }

        // The server-side UNLISTEN of a channel left without handlers is
        // lazy: queued here, sent by the run loop. Until it lands, arriving
        // notifications find no handler and are dropped.
        bool remove_handler(const HandlerHandle &h) {
            std::vector<std::shared_ptr<Command> > completed;
            bool removed = false;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                if (h.wildcard) {
                    auto it = wildcard_.find(h.channel);
                    if (it != wildcard_.end()) {
                        removed = erase_handler(it->second, h.id);
                        if (it->second.empty()) wildcard_.erase(it);
                    }
                } else {
                    removed = erase_exact_locked(h.channel, h.id, completed);
                }
            }
            finish_commands(completed);
            return removed;
        }

        bool remove_channel(const std::string &channel) {
            std::vector<std::shared_ptr<Command> > completed;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                if (is_wildcard(channel)) return wildcard_.erase(channel) > 0;
                if (exact_.erase(channel) == 0) return false;
                channels_.erase(channel);
                queue_unlisten_locked(channel, completed);
            }
            finish_commands(completed);
            return true;
        }

        usub::uvent::task::Awaitable<void> run() {
            // add_handler() calls made before run() own the connection until
            // their LISTEN is answered.
            for (;;) {
                std::vector<std::shared_ptr<Command> > completed;
                bool started = false;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    if (direct_busy_ == 0) {
                        started = running_ = true;
                        conn_ready_ = conn_ && conn_->connected();
                        start_next_command_locked(completed);
                    }
                }
                finish_commands(completed);
                if (started) break;
                const bool slept = co_await detail::sleep_for(std::chrono::milliseconds(1));
                if (!slept) co_return;  // canceled
            }

            std::vector<Notification> batch;
            std::vector<std::shared_ptr<Command> > completed;
            for (;;) {
                bool alive = false;
                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    alive = conn_ && conn_->connected();
                    if (!alive) connection_lost_locked();
                }
                if (!alive) {
                    const bool ok = co_await reconnect();
                    if (!ok) co_return;  // canceled
                    continue;
                }

                // conn_ is only ever replaced by this loop: no lock needed to
                // read it here.
                co_await conn_->wait_readable_for_listener();
                if (detail::cancel_requested()) co_return;

                {
                    std::lock_guard<std::mutex> lk(mtx_);
                    drain_locked(batch, completed);
                }
                finish_commands(completed);
                for (auto &n : batch) dispatch(std::move(n));
                batch.clear();
            }
        }

        Stats stats() const {
            std::lock_guard<std::mutex> lk(mtx_);
            Stats s;
            for (auto const &kv : channels_) {
                std::lock_guard<std::mutex> qlk(kv.second->mtx);
                s.waiting += kv.second->waiting.size();
            }
            return s;
        }

    private:
        using Handlers = std::vector<std::pair<uint64_t, std::shared_ptr<IPgNotifyHandler> > >;

        struct Notification {
            std::string channel;
            std::string payload;
            int pid;
        };

        // A LISTEN / UNLISTEN waiting for, or in, its round trip.
        struct Command {
            Command(std::string ch, bool l) : channel(std::move(ch)), listen(l) {}

            std::string channel;
            bool listen;
            bool done{false};  // under mtx_
            bool ok{false};    // under mtx_
            usub::uvent::sync::AsyncEvent finished{usub::uvent::sync::Reset::Manual};
        };

        // A notification and the handlers it was routed to on arrival.
        struct Delivery {
            Notification n;
            std::vector<std::shared_ptr<IPgNotifyHandler> > handlers;
        };

        // Per channel (exact, or matched by a wildcard): handler runs in
        // flight and the notifications waiting for a free slot. Shared with
        // the runs themselves, which start the next waiting notification as
        // they finish, so it outlives the multiplexer if it has to.
        struct ChannelQueue {
            explicit ChannelQueue(size_t l) : limit(l) {}

            std::mutex mtx;
            std::deque<Delivery> waiting;
            size_t running{0};
            const size_t limit;  // 0 = no limit
        };

        static bool is_wildcard(const std::string &ch) {
            const size_t n = ch.size();
            return n >= 2 && ch[n - 1] == '*' && ch[n - 2] == '.';
        }

        static bool wildcard_matches(const std::string &pattern, const std::string &channel) {
            return channel.compare(0, pattern.size() - 1, pattern, 0, pattern.size() - 1) == 0;
        }

        static bool is_simple_ident(const std::string &s) {
            if (s.empty()) return false;
            const unsigned char c0 = static_cast<unsigned char>(s[0]);
            if (!((c0 >= 'a' && c0 <= 'z') || c0 == '_')) return false;
            for (size_t i = 1; i < s.size(); ++i) {
                const unsigned char c = static_cast<unsigned char>(s[i]);
                if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) return false;
            }
            return true;
        }

        static std::string quote_ident_channel(const std::string &ch) {
            if (is_simple_ident(ch)) return ch;
            std::string out;
            out.reserve(ch.size() + 2);
            out.push_back('"');
            for (char c : ch) {
                if (c == '"') out.push_back('"');
                out.push_back(c);
            }
            out.push_back('"');
            return out;
        }

        static std::string statement(bool listen, const std::string &channel) {
            return (listen ? "LISTEN " : "UNLISTEN ") + quote_ident_channel(channel) + ";";
        }

        static bool erase_handler(Handlers &handlers, uint64_t id) {
            for (auto it = handlers.begin(); it != handlers.end(); ++it) {
                if (it->first == id) {
                    handlers.erase(it);
                    return true;
                }
            }
            return false;
        }

        std::shared_ptr<ChannelQueue> channel_state_locked(const std::string &channel) {
            auto &slot = channels_[channel];
            if (!slot) slot = std::make_shared<ChannelQueue>(cfg_.channel_queue_capacity);
            return slot;
        }

        // Drops handler `id` of an exact channel; the last one out queues
        // the UNLISTEN. Lock held.
        bool erase_exact_locked(const std::string &channel, uint64_t id,
                                std::vector<std::shared_ptr<Command> > &completed) {
            auto it = exact_.find(channel);
            if (it == exact_.end() || !erase_handler(it->second, id)) return false;
            if (it->second.empty()) {
                exact_.erase(it);
                channels_.erase(channel);
                queue_unlisten_locked(channel, completed);
            }
            return true;
        }

        void queue_unlisten_locked(const std::string &channel, std::vector<std::shared_ptr<Command> > &completed) {
            commands_.push_back(std::make_shared<Command>(channel, false));
            if (running_) start_next_command_locked(completed);
        }

        // Sends the next queued statement unless one is in flight or the run
        // loop has no subscribed connection; the reply wakes the run loop,
        // which completes it and calls this again. A statement superseded by a
        // later change of its channel completes without a round trip. Lock
        // held; finished commands go to `completed`, signalled once the lock
        // is released (finish_commands).
        void start_next_command_locked(std::vector<std::shared_ptr<Command> > &completed) {
            while (!inflight_ && conn_ready_ && !commands_.empty()) {
                auto cmd = std::move(commands_.front());
                commands_.pop_front();
                if (cmd->listen != (exact_.find(cmd->channel) != exact_.end())) {
                    cmd->ok = cmd->done = true;
                    completed.push_back(std::move(cmd));
                    continue;
                }
                PGconn *raw = conn_->raw_conn();
                const std::string sql = statement(cmd->listen, cmd->channel);
                // A failed send, or a statement of a few dozen bytes the kernel
                // would not take whole (every earlier one has been answered and
                // nothing else is ever written), means a broken connection:
                // shut the socket down so the parked run loop wakes, finds it
                // dead and reconnects; the statement is sent again afterwards.
                if (!raw || PQsendQuery(raw, sql.c_str()) == 0 || PQflush(raw) != 0) {
                    commands_.push_front(std::move(cmd));
                    conn_ready_ = false;
                    if (raw && PQsocket(raw) >= 0) ::shutdown(PQsocket(raw), SHUT_RDWR);
                    return;
                }
                inflight_ = std::move(cmd);
                inflight_ok_ = true;
            }
        }

        // Run loop, after a wakeup: what the socket holds goes into libpq,
        // notifications into `batch`, a finished statement into `completed`.
        // Lock held.
        void drain_locked(std::vector<Notification> &batch, std::vector<std::shared_ptr<Command> > &completed) {
            PGconn *raw = conn_->raw_conn();
            if (!raw) return;
            // On failure libpq marks the connection bad; what it parsed before
            // is still delivered, and the next pass reconnects.
            (void) PQconsumeInput(raw);
            while (PGnotify *n = PQnotifies(raw)) {
                batch.push_back(Notification{n->relname ? n->relname : "", n->extra ? n->extra : "", n->be_pid});
                PQfreemem(n);
            }
            while (inflight_ && !PQisBusy(raw)) {
                PGresult *r = PQgetResult(raw);
                if (!r) {
                    inflight_->ok = inflight_ok_;
                    inflight_->done = true;
                    completed.push_back(std::move(inflight_));
                    inflight_.reset();
                    break;
                }
                if (PQresultStatus(r) != PGRES_COMMAND_OK) inflight_ok_ = false;
                PQclear(r);
            }
            start_next_command_locked(completed);
        }

        // The connection is gone: the statement in flight is sent again
        // after the reconnect. Lock held.
        void connection_lost_locked() {
            conn_ready_ = false;
            if (inflight_) {
                commands_.push_front(std::move(inflight_));
                inflight_.reset();
            }
        }

        static void finish_commands(std::vector<std::shared_ptr<Command> > &completed) {
            for (auto &c : completed) c->finished.set();
            completed.clear();
        }

        // add_handler() before run(): the caller owns the connection.
        usub::uvent::task::Awaitable<bool> listen_direct(std::string channel) {
            if (!conn_ || !conn_->connected()) co_return false;
            const QueryResult qr = co_await conn_->exec_simple_query_nonblocking(statement(true, channel));
            co_return qr.ok;
        }

        usub::uvent::task::Awaitable<bool> reconnect() {
            const auto backoff = std::chrono::microseconds(cfg_.reconnect_backoff_us);
            std::vector<std::shared_ptr<Command> > completed;
            for (;;) {
                auto fresh = std::make_shared<PgConnectionLibpq>();
                auto conninfo = make_conninfo(host_, port_, user_, db_, password_, ssl_config_, keepalive_config_);
                if (conninfo) {
                    auto err = co_await fresh->connect_async(conninfo.value());
                    if (!err) {
                        {
                            std::lock_guard<std::mutex> lk(mtx_);
                            conn_ = fresh;
                        }
                        const bool subscribed = co_await resubscribe_all();
                        if (subscribed) {
                            {
                                std::lock_guard<std::mutex> lk(mtx_);
                                conn_ready_ = true;
                                start_next_command_locked(completed);
                            }
                            finish_commands(completed);
                            co_return true;
                        }
                    }
                }
                const bool slept = co_await detail::sleep_for(backoff);
                if (!slept) co_return false;
            }
        }

        // Run loop only, while conn_ready_ is false (nobody else sends).
        usub::uvent::task::Awaitable<bool> resubscribe_all() {
            std::vector<std::string> names;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                names.reserve(exact_.size());
                for (auto const &kv : exact_) names.push_back(kv.first);
            }
            for (auto const &name : names) {
                const QueryResult qr = co_await conn_->exec_simple_query_nonblocking(statement(true, name));
                if (!qr.ok) co_return false;
            }
            co_return true;
        }

        void dispatch(Notification n) {
            Delivery d;
            std::shared_ptr<ChannelQueue> q;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                if (auto it = exact_.find(n.channel); it != exact_.end())
                    for (auto &h : it->second) d.handlers.push_back(h.second);
                for (auto &kv : wildcard_)
                    if (wildcard_matches(kv.first, n.channel))
                        for (auto &h : kv.second) d.handlers.push_back(h.second);
                if (d.handlers.empty()) return;
                q = channel_state_locked(n.channel);
            }
            d.n = std::move(n);
            std::vector<Delivery> ready;
            {
                std::lock_guard<std::mutex> lk(q->mtx);
                q->waiting.push_back(std::move(d));
                take_ready_locked(*q, ready);
            }
            start(q, ready);
        }

        // Moves waiting notifications out while the channel has free slots.
        // Queue lock held.
        static void take_ready_locked(ChannelQueue &q, std::vector<Delivery> &ready) {
            while (!q.waiting.empty() && (q.limit == 0 || q.running < q.limit)) {
                q.running += q.waiting.front().handlers.size();
                ready.push_back(std::move(q.waiting.front()));
                q.waiting.pop_front();
            }
        }

        static void start(const std::shared_ptr<ChannelQueue> &q, std::vector<Delivery> &ready) {
            for (auto &d : ready)
                for (auto &h : d.handlers)
                    usub::uvent::system::co_spawn(run_handler(h, q, d.n.channel, d.n.payload, d.n.pid));
            ready.clear();
        }

        // Detached: everything by value, nothing escapes. A finished run
        // hands its slot to the next waiting notification.
        static usub::uvent::task::Awaitable<void>
        run_handler(std::shared_ptr<IPgNotifyHandler> handler, std::shared_ptr<ChannelQueue> q,
                    std::string channel, std::string payload, int pid) {
            try {
                co_await (*handler)(channel, std::move(payload), pid);
            } catch (const std::exception &e) {
                std::fprintf(stderr, "[UPQ/notify] handler for channel %s threw: %s\n", channel.c_str(), e.what());
            } catch (...) {
                std::fprintf(stderr, "[UPQ/notify] handler for channel %s threw\n", channel.c_str());
            }
            std::vector<Delivery> ready;
            {
                std::lock_guard<std::mutex> lk(q->mtx);
                --q->running;
                take_ready_locked(*q, ready);
            }
            start(q, ready);
        }

        std::shared_ptr<PgConnectionLibpq> conn_;
        std::string host_;
        std::string port_;
        std::string user_;
        std::string db_;
        std::string password_;
        SSLConfig ssl_config_;
        TCPKeepaliveConfig keepalive_config_;
        Config cfg_;

        std::atomic<uint64_t> next_handler_id_{1};

        mutable std::mutex mtx_;  // everything below, and every libpq call on conn_ once run() owns it
        std::unordered_map<std::string, Handlers> exact_;
        std::unordered_map<std::string, Handlers> wildcard_;
        std::unordered_map<std::string, std::shared_ptr<ChannelQueue> > channels_;
        std::deque<std::shared_ptr<Command> > commands_;
        std::shared_ptr<Command> inflight_;
        bool inflight_ok_{true};
        bool running_{false};     // run() owns the connection
        bool conn_ready_{false};  // ... and it is subscribed: statements may be sent
        int direct_busy_{0};      // add_handler() LISTENs running before run()
    };
} // namespace usub::pg

#endif
