#ifndef PGNOTIFICATIONMULTIPLEXER_H
#define PGNOTIFICATIONMULTIPLEXER_H

#include <libpq-fe.h>

#ifdef _WIN32
#  include <winsock2.h>
#else
#  include <sys/ioctl.h>
#endif

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
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
#include "uvent/Uvent.h"
#include "uvent/utils/datastructures/queue/ConcurrentQueues.h"

namespace usub::pg {
    struct IPgNotifyHandler {
        virtual usub::uvent::task::Awaitable<void> operator()(std::string channel,
                                                              std::string payload,
                                                              int backend_pid) = 0;

        virtual ~IPgNotifyHandler() = default;
    };

    class PgNotificationMultiplexer {
    public:
        struct Config {
            size_t channel_queue_capacity;
            size_t pending_after_disconnect_capacity;
            uint64_t reconnect_backoff_us;
            uint32_t max_recursive_depth;
            uint32_t rate_limit_per_sec;

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
              cfg_(cfg),
              keepalive_config_(keepalive_config) {
            next_handler_id_.store(1, std::memory_order_relaxed);
        }

        usub::uvent::task::Awaitable<std::optional<HandlerHandle> > add_handler(
            const std::string &channel, std::shared_ptr<IPgNotifyHandler> handler) {
            uint64_t hid = next_handler_id_.fetch_add(1, std::memory_order_relaxed);

            bool is_wild = is_wildcard(channel);
            if (is_wild) {
                {
                    std::lock_guard<std::mutex> lk(state_mtx_);
                    auto &info = wildcard_[channel];
                    info.handlers.emplace_back(hid, std::move(handler));
                }
                co_return HandlerHandle{hid, channel, true};
            }

            bool first_for_channel = false;
            {
                std::lock_guard<std::mutex> lk(state_mtx_);
                auto &ci = exact_[channel];
                first_for_channel = ci.handlers.empty();
                ci.handlers.emplace_back(hid, std::move(handler));
                ensure_channel_runtime(channel);
            }

            if (first_for_channel) {
                if (!(co_await listen_channel(channel))) {
                    co_return std::nullopt;
                }
            }

            co_return HandlerHandle{hid, channel, false};
        }

        bool remove_handler(const HandlerHandle &h) {
            std::lock_guard<std::mutex> lk(state_mtx_);
            if (h.wildcard) {
                auto it = wildcard_.find(h.channel);
                if (it == wildcard_.end()) return false;
                auto &vec = it->second.handlers;
                for (auto vit = vec.begin(); vit != vec.end(); ++vit) {
                    if (vit->first == h.id) {
                        vec.erase(vit);
                        if (vec.empty()) {
                            wildcard_.erase(it);
                        }
                        return true;
                    }
                }
                return false;
            }

            auto it = exact_.find(h.channel);
            if (it == exact_.end()) return false;
            auto &vec = it->second.handlers;
            for (auto vit = vec.begin(); vit != vec.end(); ++vit) {
                if (vit->first == h.id) {
                    vec.erase(vit);
                    if (vec.empty()) {
                        unlisten_channel_sync(h.channel);
                        exact_.erase(it);
                        channel_runtime_.erase(h.channel);
                    }
                    return true;
                }
            }
            return false;
        }

        bool remove_channel(const std::string &channel) {
            std::lock_guard<std::mutex> lk(state_mtx_);
            bool is_wild = is_wildcard(channel);
            if (is_wild) {
                auto it = wildcard_.find(channel);
                if (it == wildcard_.end()) return false;
                wildcard_.erase(it);
                return true;
            }

            auto it = exact_.find(channel);
            if (it == exact_.end()) return false;
            unlisten_channel_sync(channel);
            exact_.erase(it);
            channel_runtime_.erase(channel);
            return true;
        }

        usub::uvent::task::Awaitable<void> run() {
            if (!ensure_connected()) {
                bool ok = co_await try_reconnect_loop();
                if (!ok) co_return;
            }

            start_channel_workers();

            while (true) {
                if (!conn_ || !conn_->connected()) {
                    bool ok = co_await try_reconnect_loop();
                    if (!ok) co_return;
                    start_channel_workers();
                }

                co_await conn_->wait_readable_for_listener();

                PGconn *raw = conn_->raw_conn();
                if (!raw) {
                    continue;
                }

                // The poller is edge-triggered, so a single PQconsumeInput per
                // wakeup is not enough: when a NOTIFY burst is larger than one
                // read (~16 KiB), the rest stays in the kernel socket buffer,
                // no new edge ever fires, and the tail of the burst is stuck
                // until an unrelated notification arrives. Drain the socket
                // completely before waiting again.
                for (;;) {
                    if (PQconsumeInput(raw) == 0) {
                        break;
                    }

                    while (true) {
                        PGnotify *n = PQnotifies(raw);
                        if (!n) break;

                        const char *ch_raw = n->relname ? n->relname : "";
                        const char *pl_raw = n->extra ? n->extra : "";
                        int be_pid = n->be_pid;

                        dispatch_event(ch_raw, pl_raw, be_pid);

                        PQfreemem(n);
                    }

#ifdef _WIN32
                    u_long pending = 0;
                    if (ioctlsocket(PQsocket(raw), FIONREAD, &pending) != 0 || pending == 0) {
                        break;
                    }
#else
                    int pending = 0;
                    if (ioctl(PQsocket(raw), FIONREAD, &pending) != 0 || pending <= 0) {
                        break;
                    }
#endif
                }
            }

            co_return;
        }

        struct Stats {
            uint64_t dropped_overflow = 0;
            uint64_t dropped_recursive = 0;
            uint64_t dropped_rate_limited = 0;
        };

        Stats stats() const {
            Stats s;
            for (auto const &kv: channel_runtime_) {
                s.dropped_overflow += kv.second.dropped_overflow.load(std::memory_order_relaxed);
                s.dropped_recursive += kv.second.dropped_recursive.load(std::memory_order_relaxed);
                s.dropped_rate_limited +=
                        kv.second.dropped_rate_limited.load(std::memory_order_relaxed);
            }
            return s;
        }

    private:
        struct ChannelInfo {
            std::vector<std::pair<uint64_t, std::shared_ptr<IPgNotifyHandler> > > handlers;
        };

        struct WildcardInfo {
            std::vector<std::pair<uint64_t, std::shared_ptr<IPgNotifyHandler> > > handlers;
        };

        struct PendingEvent {
            std::string channel;
            std::string payload;
            int pid;
        };

        struct ChannelRuntimeState {
            usub::queue::concurrent::MPMCQueue<PendingEvent> queue;
            std::atomic<bool> worker_running{false};
            std::atomic<uint64_t> dropped_overflow{0};
            std::atomic<uint64_t> dropped_recursive{0};
            std::atomic<uint64_t> dropped_rate_limited{0};
            std::atomic<uint64_t> last_tick_ns{0};
            std::atomic<uint32_t> tick_count{0};

            ChannelRuntimeState(size_t cap) : queue(cap) {
                last_tick_ns.store(now_ns(), std::memory_order_relaxed);
                tick_count.store(0, std::memory_order_relaxed);
            }

            static uint64_t now_ns() {
                auto n = std::chrono::steady_clock::now().time_since_epoch();
                return (uint64_t) std::chrono::duration_cast<std::chrono::nanoseconds>(n).count();
            }

            ChannelRuntimeState(const ChannelRuntimeState &) = delete;

            ChannelRuntimeState &operator=(const ChannelRuntimeState &) = delete;
        };

        inline static thread_local uint32_t tls_dispatch_depth = 0;
        inline static thread_local std::string tls_last_channel;
        inline static thread_local std::string tls_last_payload;

        static bool is_wildcard(const std::string &ch) {
            size_t n = ch.size();
            if (n < 2) return false;
            if (ch.back() != '*') return false;
            if (ch[n - 2] != '.') return false;
            return true;
        }

        static bool is_simple_ident(const std::string &s) {
            if (s.empty()) return false;
            unsigned char c0 = (unsigned char) s[0];
            if (!((c0 >= 'a' && c0 <= 'z') || c0 == '_')) return false;

            for (size_t i = 1; i < s.size(); ++i) {
                unsigned char c = (unsigned char) s[i];
                bool ok = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '_');
                if (!ok) return false;
            }

            return true;
        }

        static std::string quote_ident_channel(const std::string &ch) {
            if (is_simple_ident(ch)) return ch;

            std::string out;
            out.reserve(ch.size() + 2);
            out.push_back('"');
            for (char c: ch) {
                if (c == '"') out.push_back('"');
                out.push_back(c);
            }
            out.push_back('"');
            return out;
        }

        bool ensure_connected() const {
            if (!conn_) return false;
            if (!conn_->connected()) return false;
            return true;
        }

        usub::uvent::task::Awaitable<bool> try_reconnect_loop() {
            using namespace std::chrono_literals;

            for (;;) {
                auto newConn = std::make_shared<PgConnectionLibpq>();

                auto conninfo = make_conninfo(this->host_, this->port_, this->user_, this->db_, this->password_,
                                              this->ssl_config_, this->keepalive_config_);
                if (!conninfo) {
                    co_await usub::uvent::system::this_coroutine::sleep_for(
                        std::chrono::microseconds(cfg_.reconnect_backoff_us));
                    continue;
                }

                auto err = co_await newConn->connect_async(conninfo.value());
                if (!err.has_value()) {
                    conn_ = newConn;

                    bool ok = co_await resubscribe_all();
                    if (ok) {
                        flush_pending_after_disconnect();
                        co_return true;
                    }
                }

                co_await usub::uvent::system::this_coroutine::sleep_for(
                    std::chrono::microseconds(cfg_.reconnect_backoff_us));
            }
        }

        usub::uvent::task::Awaitable<bool> resubscribe_all() {
            std::vector<std::string> names;
            {
                std::lock_guard<std::mutex> lk(state_mtx_);
                names.reserve(exact_.size());
                for (auto const &kv : exact_) names.push_back(kv.first);
            }
            for (auto const &name : names) {
                std::string sql = "LISTEN " + quote_ident_channel(name) + ";";
                QueryResult qr = co_await conn_->exec_simple_query_nonblocking(sql);
                if (!qr.ok) {
                    co_return false;
                }
            }
            co_return true;
        }

        void flush_pending_after_disconnect() {
            std::deque<PendingEvent> drained;
            {
                std::lock_guard<std::mutex> lk(state_mtx_);
                drained.swap(pending_after_disconnect_);
            }
            for (auto &ev : drained) {
                dispatch_event(ev.channel, ev.payload, ev.pid);
            }
        }

        usub::uvent::task::Awaitable<bool> listen_channel(const std::string &channel) {
            std::string sql = "LISTEN " + quote_ident_channel(channel) + ";";
            QueryResult qr = co_await conn_->exec_simple_query_nonblocking(sql);
            if (!qr.ok) {
                co_return false;
            }
            co_return true;
        }

        void unlisten_channel_sync(const std::string &channel) {
            if (!ensure_connected()) return;
            std::string sql = "UNLISTEN " + quote_ident_channel(channel) + ";";
            usub::uvent::system::co_spawn(conn_->exec_simple_query_nonblocking(sql));
        }

        void ensure_channel_runtime(const std::string &channel) {
            if (channel_runtime_.find(channel) != channel_runtime_.end()) return;

            channel_runtime_.emplace(std::piecewise_construct, std::forward_as_tuple(channel),
                                     std::forward_as_tuple(cfg_.channel_queue_capacity));
        }

        void start_channel_workers() {
            std::vector<std::string> to_start;
            {
                std::lock_guard<std::mutex> lk(state_mtx_);
                for (auto &kv : channel_runtime_) {
                    auto &state = kv.second;
                    bool expected = false;
                    if (state.worker_running.compare_exchange_strong(
                            expected, true, std::memory_order_acq_rel,
                            std::memory_order_relaxed)) {
                        to_start.push_back(kv.first);
                    }
                }
            }
            for (auto &ch_copy : to_start) {
                usub::uvent::system::co_spawn(channel_worker(ch_copy, this));
            }
        }

        void dispatch_event(std::string_view ch, std::string_view payload, int pid) {
            if (!ensure_connected()) {
                std::lock_guard<std::mutex> lk(state_mtx_);
                if (pending_after_disconnect_.size() < cfg_.pending_after_disconnect_capacity) {
                    pending_after_disconnect_.push_back(
                        PendingEvent{std::string(ch), std::string(payload), pid});
                }
                return;
            }

            ChannelRuntimeState *state_ptr = nullptr;
            std::string ch_str(ch);

            {
                std::lock_guard<std::mutex> lk(state_mtx_);

                auto it = channel_runtime_.find(ch_str);
                if (it == channel_runtime_.end()) {
                    auto eit = exact_.find(ch_str);
                    if (eit == exact_.end() && !match_any_wildcard_locked(ch_str)) {
                        return;
                    }

                    auto insert_res = channel_runtime_.emplace(
                        std::piecewise_construct, std::forward_as_tuple(ch_str),
                        std::forward_as_tuple(cfg_.channel_queue_capacity));

                    it = insert_res.first;

                    bool expected = false;
                    if (it->second.worker_running.compare_exchange_strong(
                            expected, true, std::memory_order_acq_rel,
                            std::memory_order_relaxed)) {
                        std::string ch_copy2 = it->first;
                        usub::uvent::system::co_spawn(channel_worker(ch_copy2, this));
                    }
                }
                state_ptr = &it->second;
            }

            PendingEvent ev{std::move(ch_str), std::string(payload), pid};

            if (!push_rate_limited(*state_ptr, ev)) {
                state_ptr->dropped_rate_limited.fetch_add(1, std::memory_order_relaxed);
                return;
            }

            if (!state_ptr->queue.try_enqueue(std::move(ev))) {
                state_ptr->dropped_overflow.fetch_add(1, std::memory_order_relaxed);
                return;
            }
        }

        bool push_rate_limited(ChannelRuntimeState &st, const PendingEvent &) {
            uint64_t now = ChannelRuntimeState::now_ns();
            uint64_t last = st.last_tick_ns.load(std::memory_order_relaxed);

            if (now - last > 1000000000ull) {
                st.last_tick_ns.store(now, std::memory_order_relaxed);
                st.tick_count.store(1, std::memory_order_relaxed);
                return true;
            }
            for (;;) {
                uint32_t cur = st.tick_count.load(std::memory_order_relaxed);
                if (cur >= cfg_.rate_limit_per_sec) {
                    return false;
                }
                if (st.tick_count.compare_exchange_weak(
                        cur, cur + 1,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    return true;
                }
            }
        }

        static usub::uvent::task::Awaitable<void> channel_worker(std::string channel_name,
                                                                 PgNotificationMultiplexer *self) {
            using namespace std::chrono_literals;

            for (;;) {
                ChannelRuntimeState *st_ptr = nullptr;
                {
                    std::lock_guard<std::mutex> lk(self->state_mtx_);
                    auto it = self->channel_runtime_.find(channel_name);
                    if (it == self->channel_runtime_.end()) {
                        co_return;
                    }
                    st_ptr = &it->second;
                }

                PendingEvent ev;
                if (!st_ptr->queue.try_dequeue(ev)) {
                    co_await usub::uvent::system::this_coroutine::sleep_for(100us);
                    continue;
                }

                self->dispatch_to_handlers_ordered(ev, *st_ptr);
            }

            co_return;
        }

        void dispatch_to_handlers_ordered(const PendingEvent &ev, ChannelRuntimeState &st) {
            if (!check_recursion(ev)) {
                st.dropped_recursive.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            DispatchDepthGuard depth_guard;

            std::vector<std::shared_ptr<IPgNotifyHandler>> snapshot;
            {
                std::lock_guard<std::mutex> lk(state_mtx_);

                auto eit = exact_.find(ev.channel);
                if (eit != exact_.end()) {
                    for (auto &pair : eit->second.handlers) {
                        snapshot.push_back(pair.second);
                    }
                }

                for (auto &kv : wildcard_) {
                    const std::string &pat = kv.first;
                    if (pat.size() >= 2 && pat.back() == '*' && pat[pat.size() - 2] == '.') {
                        std::string prefix = pat.substr(0, pat.size() - 1);
                        if (ev.channel.rfind(prefix, 0) == 0) {
                            for (auto &pair : kv.second.handlers) {
                                snapshot.push_back(pair.second);
                            }
                        }
                    }
                }
            }

            if (snapshot.empty()) {
                return;
            }

            for (auto &hptr : snapshot) {
                std::string ch_copy = ev.channel;
                std::string pl_copy = ev.payload;
                int pid_copy = ev.pid;
                usub::uvent::system::co_spawn(
                    run_single_handler(hptr, std::move(ch_copy), std::move(pl_copy), pid_copy));
            }
        }

        struct DispatchDepthGuard {
            DispatchDepthGuard() { ++tls_dispatch_depth; }
            ~DispatchDepthGuard() { --tls_dispatch_depth; }
            DispatchDepthGuard(const DispatchDepthGuard&) = delete;
            DispatchDepthGuard& operator=(const DispatchDepthGuard&) = delete;
        };

        bool check_recursion(const PendingEvent &ev) {
            if (tls_dispatch_depth >= cfg_.max_recursive_depth) {
                if (tls_last_channel == ev.channel && tls_last_payload == ev.payload) {
                    return false;
                }
            }
            tls_last_channel = ev.channel;
            tls_last_payload = ev.payload;
            return true;
        }

        bool match_any_wildcard_locked(const std::string &ch) const {
            for (auto const &kv: wildcard_) {
                const std::string &pat = kv.first;
                if (pat.size() >= 2 && pat.back() == '*' && pat[pat.size() - 2] == '.') {
                    std::string prefix = pat.substr(0, pat.size() - 1);
                    if (ch.rfind(prefix, 0) == 0) return true;
                }
            }
            return false;
        }

        bool match_any_wildcard(const std::string &ch) const {
            std::lock_guard<std::mutex> lk(state_mtx_);
            return match_any_wildcard_locked(ch);
        }

        bool get_exact_handlers(const std::string &ch, ChannelInfo *&out) {
            std::lock_guard<std::mutex> lk(state_mtx_);
            auto it = exact_.find(ch);
            if (it == exact_.end()) {
                return false;
            }
            out = &it->second;
            return true;
        }

        bool get_wild_handlers(const std::string &ch, std::vector<WildcardInfo *> &out_list) {
            std::lock_guard<std::mutex> lk(state_mtx_);
            bool any = false;
            for (auto &kv: wildcard_) {
                const std::string &pat = kv.first;
                if (pat.size() >= 2 && pat.back() == '*' && pat[pat.size() - 2] == '.') {
                    std::string prefix = pat.substr(0, pat.size() - 1);
                    if (ch.rfind(prefix, 0) == 0) {
                        out_list.push_back(&kv.second);
                        any = true;
                    }
                }
            }
            return any;
        }

        static usub::uvent::task::Awaitable<void> run_single_handler(
            std::shared_ptr<IPgNotifyHandler> hptr, std::string ch_copy, std::string payload_copy,
            int pid_copy) {
            co_await (*hptr)(std::move(ch_copy), std::move(payload_copy), pid_copy);
            co_return;
        }

    private:
        std::shared_ptr<PgConnectionLibpq> conn_;
        std::string host_;
        std::string port_;
        std::string user_;
        std::string db_;
        std::string password_;
        SSLConfig ssl_config_;
        TCPKeepaliveConfig keepalive_config_;
        Config cfg_;

        std::unordered_map<std::string, ChannelInfo> exact_;
        std::unordered_map<std::string, WildcardInfo> wildcard_;

        std::unordered_map<std::string, ChannelRuntimeState> channel_runtime_;

        std::deque<PendingEvent> pending_after_disconnect_;

        std::atomic<uint64_t> next_handler_id_;

        mutable std::mutex state_mtx_;
    };
} // namespace usub::pg

#endif
