#ifndef PGCONNECTIONLIBPQ_H
#define PGCONNECTIONLIBPQ_H

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <cstring>
#include <expected>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <libpq-fe.h>

#include "PgJsonCodec.h"
#include "PgParams.h"
#include "PgReflect.h"
#include "PgTypes.h"
#include "meta/PgConcepts.h"
#include "uvent/Uvent.h"
#include "uvent/net/AwaiterOperations.h"
#include "utils/UventCompat.h"

// UPQ_REFLECT_DEBUG defaults to 0 in PgReflect.h (included above).
#if UPQ_REFLECT_DEBUG
#include <cstdio>
#define UPQ_CONN_DBG(fmt, ...) \
    do { std::fprintf(stderr, "[UPQ/conn] " fmt "\n", ##__VA_ARGS__); } while (0)
#else
#define UPQ_CONN_DBG(fmt, ...) \
    do { } while (0)
#endif

namespace usub::pg {
    // Copies a server ERROR (SQLSTATE, primary message, detail, hint) into a
    // query result and marks it failed.
    void fill_server_error_fields(PGresult *res, QueryResult &out);

    // rows_affected of one command result (CommandComplete tag).
    uint64_t extract_rows_affected(PGresult *res);

    enum class SSLMode {
        disable,
        allow,
        prefer,
        require,
        verify_ca,
        verify_full
    };

    static inline std::string to_string(SSLMode m) {
        switch (m) {
            case SSLMode::disable: return "disable";
            case SSLMode::allow: return "allow";
            case SSLMode::prefer: return "prefer";
            case SSLMode::require: return "require";
            case SSLMode::verify_ca: return "verify-ca";
            case SSLMode::verify_full: return "verify-full";
        }
        return "prefer";
    }

    struct SSLConfig {
        SSLMode mode{SSLMode::prefer};

        // For verify-ca / verify-full
        std::optional<std::string> root_cert; // CA bundle (path or PEM)
        std::optional<std::string> crl; // optional

        // For mTLS (client auth)
        std::optional<std::string> client_cert; // path or PEM
        std::optional<std::string> client_key; // path or PEM

        // For verify-full when connecting by IP but validating DNS name
        std::optional<std::string> server_hostname;
    };

    struct TCPKeepaliveConfig {
        bool enabled{false};

        int keepalives{1}; // 0/1
        int idle{30}; // idle before probes
        int interval{10}; // between probes
        int count{3}; // probes before drop
    };

    class PgPool;
    class PgTransaction;

    namespace detail {
        // A statement handed to run_query(): something that queues it on the
        // connection (PQsendQuery / PQsendQueryParams) and returns libpq's
        // verdict. Sql is either `const std::string *` (the caller keeps the
        // text alive across the await, as with every `const std::string &`
        // API) or `std::string` (owned by the coroutine frame).
        inline const char *sql_cstr(const std::string *s) noexcept { return s->c_str(); }
        inline const char *sql_cstr(const std::string &s) noexcept { return s.c_str(); }

        template<class Sql>
        struct SimpleSend {
            Sql sql;

            int operator()(PGconn *c) const { return PQsendQuery(c, sql_cstr(sql)); }
        };

        // Arguments are held by reference: like the coroutine parameters they
        // replace, they must outlive the co_await of the returned Awaitable
        // (they do when it is awaited in the same full-expression).
        template<class Sql, class... Args>
        struct ParamSend {
            Sql sql;
            std::tuple<Args &&...> args;

            int operator()(PGconn *c) const {
                return std::apply([&](const auto &... a) { return send_query_params(c, sql_cstr(sql), a...); },
                                  args);
            }
        };

        template<class Sql, class... Args>
        ParamSend<Sql, Args...> make_param_send(Sql sql, Args &&... args) {
            return ParamSend<Sql, Args...>{std::move(sql), std::tuple<Args &&...>(std::forward<Args>(args)...)};
        }

        // Queues `sql` with the simple protocol when there are no arguments
        // (several statements allowed), with PQsendQueryParams otherwise.
        template<class Sql, class... Args>
        auto make_send(Sql sql, Args &&... args) {
            if constexpr (sizeof...(Args) == 0) return SimpleSend<Sql>{std::move(sql)};
            else return make_param_send(std::move(sql), std::forward<Args>(args)...);
        }

        template<class Finish>
        using finish_result_t = std::invoke_result_t<Finish &, QueryResult &&>;

        // Ready value for early-exit paths of non-coroutine API wrappers.
        template<class T>
        uvent::task::Awaitable<T> ready(T v) { co_return v; }

        inline uvent::task::Awaitable<void> ready_void() { co_return; }

        inline QueryResult closed_result(const char *why) {
            QueryResult r;
            r.ok = false;
            r.code = PgErrorCode::ConnectionClosed;
            r.error = why;
            r.rows_valid = false;
            return r;
        }

        inline QueryResult invalid_cursor_result() {
            QueryResult r;
            r.ok = false;
            r.code = PgErrorCode::ProtocolCorrupt;
            r.error = "invalid cursor name";
            r.rows_valid = false;
            return r;
        }
    } // namespace detail

    class PgConnectionLibpq {
    public:
        template<class HandlerT>
            requires PgNotifyHandler<HandlerT>
        friend class PgNotificationListener;

        friend class PgNotificationMultiplexer;
        friend class PgPool;
        friend class PgTransaction;
        friend class PgRowStream;

        PgConnectionLibpq();

        ~PgConnectionLibpq();

        usub::uvent::task::Awaitable<std::optional<std::string> >
        connect_async(const std::string &conninfo) {
            using namespace std::chrono_literals;
            return connect_async(conninfo, 5s);
        }

        usub::uvent::task::Awaitable<std::optional<std::string> >
        connect_async(const std::string &conninfo, std::chrono::milliseconds timeout);

        [[nodiscard]] bool connected() const noexcept;

        // ---------- statements ----------
        //
        // Every statement API below is one coroutine frame: a non-coroutine
        // wrapper builds the send step and the result conversion and returns
        // run_query(). `sql` / argument references must stay alive until the
        // returned Awaitable completes.

        usub::uvent::task::Awaitable<QueryResult>
        exec_simple_query_nonblocking(const std::string &sql) {
            return run_query(detail::SimpleSend<const std::string *>{&sql}, detail::AsResult{});
        }

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        exec_param_query_nonblocking(const std::string &sql, Args &&... args) {
            return run_query(detail::make_param_send(&sql, std::forward<Args>(args)...), detail::AsResult{});
        }

        template<class T>
        usub::uvent::task::Awaitable<std::vector<T> >
        exec_simple_query_nonblocking(const std::string &sql) {
            return run_query(detail::SimpleSend<const std::string *>{&sql}, detail::AsRows<T>{});
        }

        template<class T>
        usub::uvent::task::Awaitable<std::optional<T> >
        exec_simple_query_one_nonblocking(const std::string &sql) {
            return run_query(detail::SimpleSend<const std::string *>{&sql}, detail::AsOptionalRow<T>{});
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::vector<T> >
        exec_param_query_nonblocking(const std::string &sql, Args &&... args) {
            return run_query(detail::make_param_send(&sql, std::forward<Args>(args)...), detail::AsRows<T>{});
        }

        template<class T, typename... Args>
        usub::uvent::task::Awaitable<std::optional<T> >
        exec_param_query_one_nonblocking(const std::string &sql, Args &&... args) {
            return run_query(detail::make_param_send(&sql, std::forward<Args>(args)...),
                             detail::AsOptionalRow<T>{});
        }

        template<typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<QueryResult>
        exec_param_query_nonblocking(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return run_query(detail::make_param_send(std::string(sv), std::forward<Args>(args)...),
                             detail::AsResult{});
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::vector<T> >
        exec_param_query_nonblocking(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return run_query(detail::make_param_send(std::string(sv), std::forward<Args>(args)...),
                             detail::AsRows<T>{});
        }

        template<class T, typename Sql, typename... Args>
            requires (std::is_convertible_v<Sql, std::string_view>
                      && !std::is_same_v<std::decay_t<Sql>, std::string>)
        usub::uvent::task::Awaitable<std::optional<T> >
        exec_param_query_one_nonblocking(Sql &&sql, Args &&... args) {
            const std::string_view sv(sql);
            detail::assert_param_count<Args...>(sv);
            return run_query(detail::make_param_send(std::string(sv), std::forward<Args>(args)...),
                             detail::AsOptionalRow<T>{});
        }

        // ---------- COPY ----------

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_start(const std::string &sql);

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_send_chunk(const void *data, size_t len);

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_finish() { return copy_in_finish(nullptr); }

        usub::uvent::task::Awaitable<PgCopyResult> copy_in_finish(const char *error_msg);

        usub::uvent::task::Awaitable<PgCopyResult> copy_out_start(const std::string &sql);

        usub::uvent::task::Awaitable<PgWireResult<std::vector<uint8_t> > > copy_out_read_chunk();

        template<class Sink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to(const std::string &sql, Sink &&sink);

        template<class LineSink>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_to_lines(const std::string &sql, LineSink &&line_sink);

        template<class Source>
        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from(const std::string &sql, Source &&source);

        usub::uvent::task::Awaitable<PgCopyResult>
        copy_from_buffer(const std::string &sql, const void *data, size_t len);

        // ---------- snapshots ----------

        usub::uvent::task::Awaitable<std::expected<std::string, PgOpError> >
        export_snapshot();

        usub::uvent::task::Awaitable<std::optional<PgOpError> >
        set_snapshot(const std::string &snapshot_id);

        // ---------- cursors ----------

        std::string make_cursor_name();

        static bool is_safe_cursor_ident(const std::string &s);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_declare(const std::string &cursor_name, const std::string &sql);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_declare_in_tx(const std::string &cursor_name, const std::string &sql);

        template<typename... Args>
        usub::uvent::task::Awaitable<QueryResult>
        cursor_declare_in_tx_params(const std::string &cursor_name,
                                    const std::string &sql,
                                    Args &&... args) {
            if (!is_safe_cursor_ident(cursor_name))
                return detail::ready(detail::invalid_cursor_result());
            return run_query(detail::make_param_send("DECLARE " + cursor_name + " NO SCROLL CURSOR FOR " + sql,
                                                     std::forward<Args>(args)...),
                             detail::AsResult{});
        }

        usub::uvent::task::Awaitable<PgCursorChunk>
        cursor_fetch_chunk(const std::string &cursor_name, uint32_t count);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_close(const std::string &cursor_name);

        usub::uvent::task::Awaitable<QueryResult>
        cursor_close_in_tx(const std::string &cursor_name);

        // ---------- raw state ----------

        PGconn *raw_conn() noexcept;

        bool is_idle();

        void close();

        // True when the last socket await was cut short by the pool's IO
        // watchdog instead of data; the connection is unusable from then on.
        [[nodiscard]] bool io_timed_out() const noexcept {
            return io_timed_out_.load(std::memory_order_acquire);
        }

        // Every query-path socket await is bounded by this deadline so a
        // lost wakeup (edge-triggered race, peer vanishing without FIN, ...)
        // surfaces as an error instead of a forever-suspended coroutine.
        //
        // The deadline is NOT enforced with the socket's own uvent timer: that
        // timer lives in the thread_local timer wheel of whichever worker
        // arms it, while pooled connections migrate between workers, so
        // arm/update/cancel from another worker silently hit a foreign wheel
        // and left a dangling Timer* behind (heap corruption once the header
        // was freed). Instead the connection only publishes the timestamp of
        // its current await and PgPool::io_watchdog_loop() (one coroutine per
        // pool) tears down overdue awaits via ::shutdown(2), which is safe from
        // any thread and wakes the parked coroutine through the owner poller
        // (EPOLLHUP/EPOLLRDHUP).
        static constexpr uint64_t kIoDeadlineMs = 15000;

        // Per-connection override of the IO deadline, 0 = kIoDeadlineMs. Set
        // by the pool around a query that carries QueryOpts::io_deadline_ms
        // and restored afterwards, so the override never outlives the query
        // it was requested for. The watchdog reads it on every pass, so a
        // change mid-await takes effect on the next tick.
        void set_io_deadline_ms(uint64_t ms) noexcept {
            io_deadline_ms_.store(ms, std::memory_order_release);
        }

        [[nodiscard]] uint64_t io_deadline_ms() const noexcept {
            const uint64_t v = io_deadline_ms_.load(std::memory_order_acquire);
            return v ? v : kIoDeadlineMs;
        }

        // Steady-clock timestamp (ms) of the await currently in flight, 0 when
        // no query-path await is parked. Read by the pool watchdog.
        [[nodiscard]] uint64_t io_await_since_ms() const noexcept {
            return await_since_ms_.load(std::memory_order_acquire);
        }

        // Called by the pool watchdog when io_await_since_ms() is overdue:
        // marks the connection timed out and shuts the socket down so the
        // parked await wakes up and bails with "io deadline exceeded".
        // Returns true if a shutdown was issued.
        bool abort_io_from_watchdog() noexcept;

        static uint64_t steady_now_ms() noexcept;

    private:
        using SocketT = usub::uvent::net::Socket<usub::uvent::net::Proto::TCP, usub::uvent::net::Role::ACTIVE>;

        // One wait for socket readiness, awaited directly by the coroutine
        // that needs it: no frame of its own. Read waits first re-probe the
        // kernel buffer (the READY hint is cleared BEFORE the probe, as the
        // uvent wake-up protocol requires) and do not park when data, EOF or
        // a socket error is already there. Query-path waits publish their start for the IO
        // watchdog; the LISTEN wait does not (it may idle forever), nor do the
        // handshake waits (connect_async bounds those itself). Handshake
        // waits probe with poll(2): libpq may ask to write on a socket that is
        // already writable, which produces no new edge.
        class IoWait {
        public:
            enum class Kind : uint8_t { Read, Write, ListenRead, HandshakeRead, HandshakeWrite };

            IoWait(PgConnectionLibpq &c, Kind k) noexcept : c_(c), kind_(k) {}

            bool await_ready() noexcept;

            void await_suspend(std::coroutine_handle<> h);

            void await_resume() noexcept;

        private:
            PgConnectionLibpq &c_;
            Kind kind_;
        };

        IoWait wait_readable() noexcept { return IoWait{*this, IoWait::Kind::Read}; }

        IoWait wait_writable() noexcept { return IoWait{*this, IoWait::Kind::Write}; }

        IoWait wait_readable_for_listener() noexcept { return IoWait{*this, IoWait::Kind::ListenRead}; }

        // The poller watches a dup of libpq's socket, never libpq's own
        // descriptor: uvent closes the number it watches (on teardown and on
        // a HUP), libpq closes its own (PQfinish, connection loss). Sharing one
        // number closed it twice, and the second close, often deferred to the
        // worker that opened the connection, hit whichever socket the process
        // had opened in between. Re-registers when libpq has switched sockets
        // (next host, SSL fallback); false = no socket.
        bool watch_socket();

        // Shared by connect_async and its deadline timer, a detached coroutine
        // that may outlive the connection: at the deadline the timer cuts a
        // stalled handshake with shutdown(2), the wake-up the pool watchdog
        // uses too. libpq may close and reopen its socket inside
        // PQconnectPoll, so `fd` is only touched under `mtx`.
        struct ConnectDeadline {
            std::mutex mtx;
            int fd{-1};
            bool done{false};
            bool fired{false};
        };

        static usub::uvent::task::Awaitable<void>
        connect_deadline_timer(std::shared_ptr<ConnectDeadline> dl, std::chrono::milliseconds budget);

        // Non-blocking steps of a round trip. Done = finished, Wait = park on
        // the socket and call again, Failed = the connection is broken
        // (connected_ is cleared; io_error_message() says why).
        enum class Step : uint8_t { Done, Wait, Failed };

        Step flush_step();          // push queued output (PQflush)

        Step input_step();          // read until a result is ready (!PQisBusy)

        Step collect_step(QueryResult &out);  // read and fold every ready result

        // Cold-path helpers (COPY, draining a connection before reuse): the
        // same steps behind a coroutine.
        usub::uvent::task::Awaitable<bool> flush_outgoing();

        usub::uvent::task::Awaitable<bool> pump_input();

        // flush_outgoing() then pump_input(): the reply to what was queued is
        // ready to be read with PQgetResult.
        usub::uvent::task::Awaitable<bool> round_trip_input();

        // The one coroutine behind every statement: send, flush, collect, then
        // convert with `finish`. Exceptions thrown by `finish` propagate to
        // the awaiting caller.
        template<class Send, class Finish>
        usub::uvent::task::Awaitable<detail::finish_result_t<Finish> >
        run_query(Send send, Finish finish);

        void mark_await_begin() noexcept;

        void mark_await_end() noexcept;

        // libpq's own message after a failed flush/pump, or the watchdog's
        // "io deadline exceeded (...)" when the await was cut short (libpq has
        // no error of its own in that case, so the message would come out
        // empty). The watchdog variant carries waited_ms / deadline_ms and
        // pending_bytes = FIONREAD on the socket at the moment of the abort:
        // >0 means the reply DID arrive and the parked coroutine was never
        // woken (poller/wakeup bug), 0 means the peer really went silent.
        [[nodiscard]] const char *io_error_message() const noexcept;

        // Marks `out` as an IO failure (connection unusable afterwards).
        template<class Result>
        void fail_io(Result &out, const char *msg) {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = msg;
            out.err_detail.message = out.error;
            connected_ = false;
        }

        // Verdict after one step (and its socket wait): true = give up, `out`
        // says why. A canceled task abandons the reply mid-protocol, so the
        // connection is not reusable afterwards; without this check a
        // cancellation would re-park on the socket in a busy loop.
        bool after_wait(Step s, QueryResult &out) {
            if (s == Step::Failed || io_timed_out()) {
                fail_io(out, io_error_message());
                return true;
            }
            if (s == Step::Wait && detail::cancel_requested()) {
                fail_io(out, "canceled");
                out.code = PgErrorCode::AwaitCanceled;
                return true;
            }
            return false;
        }

        // Discards whatever results are still queued (after a reply has been
        // read up to !PQisBusy). Used before handing a connection back.
        void discard_pending_results();

        // Cheap liveness check of an idle pooled connection before reuse: a
        // peer that closed it meanwhile (server/pooler restart, idle timeout,
        // pg_terminate_backend) left EOF or a FATAL message in the socket.
        // One non-blocking recv(MSG_PEEK); false = drop it, the statement
        // has not been sent yet so nothing is lost.
        bool idle_socket_alive();

        PgCopyResult drain_copy_end_result();

    private:
        PGconn *conn_{nullptr};
        bool connected_{false};
        std::atomic<bool> io_timed_out_{false};
        std::atomic<uint64_t> await_since_ms_{0};
        std::atomic<uint64_t> io_deadline_ms_{0}; // 0 = kIoDeadlineMs
        // Written by the watchdog thread before the release-store of
        // io_timed_out_, read by the coroutine after its acquire-load.
        std::string io_timeout_detail_;

        std::unique_ptr<SocketT> sock_;
        // libpq's socket that sock_ (a dup of it) stands for: number + inode,
        // since a new socket often reuses the number of the one just closed.
        int watched_fd_{-1};
        uint64_t watched_ino_{0};

        uint64_t cursor_seq_{0};

        mutable std::mutex close_mtx_;

    public:
        std::atomic<bool> dead_accounted_{false};
    };

    template<class Send, class Finish>
    usub::uvent::task::Awaitable<detail::finish_result_t<Finish> >
    PgConnectionLibpq::run_query(Send send, Finish finish) {
        if (!connected()) co_return finish(detail::closed_result("connection not OK"));

        QueryResult out;
        out.ok = true;
        out.code = PgErrorCode::OK;

        bool failed = !send(conn_);
        if (failed) fail_io(out, PQerrorMessage(conn_));

        while (!failed) {
            const Step s = flush_step();
            if (s == Step::Done) break;
            if (s == Step::Wait) co_await wait_writable();
            failed = after_wait(s, out);
        }

        while (!failed) {
            const Step s = collect_step(out);
            if (s == Step::Done) break;
            if (s == Step::Wait) co_await wait_readable();
            failed = after_wait(s, out);
        }

        if (failed) {
            out.rows.clear();
            out.columns.clear();
            out.rows_valid = false;
        }
        co_return finish(std::move(out));
    }

    template<class Sink>
    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_to(const std::string &sql, Sink &&sink) {
        PgCopyResult start = co_await copy_out_start(sql);
        if (!start.ok) co_return start;

        PgCopyResult final_result{};
        final_result.ok = true;
        final_result.code = PgErrorCode::OK;

        bool user_aborted = false;
        for (;;) {
            auto chunk = co_await copy_out_read_chunk();
            if (!chunk.ok) {
                final_result.ok = false;
                final_result.code = chunk.err.code;
                final_result.error = chunk.err.message;
                co_return final_result;
            }
            if (chunk.value.empty()) break;
            if (user_aborted) continue;  // drain the stream so the connection stays usable
            const std::string_view sv(reinterpret_cast<const char *>(chunk.value.data()), chunk.value.size());
            if (!co_await sink(sv)) user_aborted = true;
        }

        if (user_aborted) {
            final_result.ok = false;
            final_result.code = PgErrorCode::AwaitCanceled;
            final_result.error = "COPY TO aborted by sink";
        }
        co_return final_result;
    }

    template<class LineSink>
    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_to_lines(const std::string &sql, LineSink &&line_sink) {
        std::string carry;

        auto chunk_sink = [&](std::string_view sv) -> usub::uvent::task::Awaitable<bool> {
            size_t pos = 0;
            while (pos < sv.size()) {
                const auto nl = sv.find('\n', pos);
                if (nl == std::string_view::npos) {
                    carry.append(sv.data() + pos, sv.size() - pos);
                    break;
                }
                std::string_view line;
                if (!carry.empty()) {
                    carry.append(sv.data() + pos, nl - pos);
                    line = std::string_view(carry);
                } else {
                    line = std::string_view(sv.data() + pos, nl - pos);
                }
                const bool keep = co_await line_sink(line);
                carry.clear();
                if (!keep) co_return false;
                pos = nl + 1;
            }
            co_return true;
        };

        PgCopyResult r = co_await copy_to(sql, chunk_sink);

        if (r.ok && !carry.empty()) {
            if (!co_await line_sink(std::string_view(carry))) {
                r.ok = false;
                r.code = PgErrorCode::AwaitCanceled;
                r.error = "COPY TO aborted by sink";
            }
            carry.clear();
        }
        co_return r;
    }

    template<class Source>
    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_from(const std::string &sql, Source &&source) {
        PgCopyResult start = co_await copy_in_start(sql);
        if (!start.ok) co_return start;

        for (;;) {
            std::string_view sv = co_await source();
            if (sv.empty()) break;
            PgCopyResult send = co_await copy_in_send_chunk(sv.data(), sv.size());
            if (!send.ok) {
                co_await copy_in_finish("aborted: send chunk failed");
                co_return send;
            }
        }
        co_return co_await copy_in_finish();
    }
} // namespace usub::pg

#endif // PGCONNECTIONLIBPQ_H
