#include "upq/PgConnection.h"

#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <fcntl.h>
#include <poll.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>

namespace usub::pg {
    // ---------- result helpers ----------

    void fill_server_error_fields(PGresult *res, QueryResult &out) {
        if (!res) return;

        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char *primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char *detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char *hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char *fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        if (sqlstate) out.err_detail.sqlstate = sqlstate;
        if (detail) out.err_detail.detail = detail;
        if (hint) out.err_detail.hint = hint;

        if (primary && *primary) out.err_detail.message = primary;
        else if (!out.error.empty()) out.err_detail.message = out.error;

        out.err_detail.category = classify_sqlstate(out.err_detail.sqlstate);

        out.ok = false;
        out.code = PgErrorCode::ServerError;
        out.rows_valid = false;
    }

    uint64_t extract_rows_affected(PGresult *res) {
        if (!res) return 0;
        if (const char *aff = PQcmdTuples(res); aff && *aff)
            return std::strtoull(aff, nullptr, 10);

        if (const char *tag = PQcmdStatus(res); tag && *tag) {
            const char *end = tag + std::strlen(tag);
            while (end > tag && std::isspace(static_cast<unsigned char>(*(end - 1)))) --end;
            const char *p = end;
            while (p > tag && std::isdigit(static_cast<unsigned char>(*(p - 1)))) --p;
            if (p < end) return std::strtoull(p, nullptr, 10);
        }
        return 0;
    }

    namespace {
        // COPY errors keep their historical one-line text ("msg [SQLSTATE x]
        // detail: .. hint: .."); err_detail is filled as well.
        void fill_copy_error(PGresult *res, PgCopyResult &out) {
            QueryResult tmp;
            fill_server_error_fields(res, tmp);
            out.ok = false;
            out.code = PgErrorCode::ServerError;
            out.error = tmp.error;
            out.err_detail = std::move(tmp.err_detail);
            if (!out.err_detail.sqlstate.empty()) out.error.append(" [SQLSTATE ").append(out.err_detail.sqlstate).append("]");
            if (!out.err_detail.detail.empty()) out.error.append(" detail: ").append(out.err_detail.detail);
            if (!out.err_detail.hint.empty()) out.error.append(" hint: ").append(out.err_detail.hint);
        }

        void append_rows(PGresult *res, std::vector<QueryResult::Row> &rows, std::vector<std::string> &columns) {
            const int nrows = PQntuples(res);
            const int ncols = PQnfields(res);

            if (columns.empty()) {
                columns.reserve(static_cast<size_t>(ncols));
                for (int c = 0; c < ncols; ++c) {
                    const char *nm = PQfname(res, c);
                    columns.emplace_back(nm ? nm : "");
                }
            }

            rows.reserve(rows.size() + static_cast<size_t>(nrows));
            for (int r = 0; r < nrows; ++r) {
                QueryResult::Row row;
                row.cols.reserve(static_cast<size_t>(ncols));
                for (int c = 0; c < ncols; ++c) {
                    if (PQgetisnull(res, r, c)) row.cols.emplace_back();
                    else row.cols.emplace_back(PQgetvalue(res, r, c), static_cast<size_t>(PQgetlength(res, r, c)));
                }
                rows.emplace_back(std::move(row));
            }
        }

        // Folds one PGresult of a statement into `out`. Several arrive for a
        // multi-statement simple query: row sets append (columns from the
        // first one), rows_affected adds up, and an ERROR replaces everything
        // gathered so far (the server skips the remaining statements).
        // Returns true when collection must stop: the connection switched to
        // COPY mode, which only the copy_* APIs can drive (PQgetResult would
        // hand out the same COPY result forever).
        bool absorb_result(PGresult *res, QueryResult &out) {
            switch (PQresultStatus(res)) {
                case PGRES_TUPLES_OK:
                case PGRES_SINGLE_TUPLE:
                    if (out.ok) {
                        append_rows(res, out.rows, out.columns);
                        out.rows_affected += static_cast<uint64_t>(PQntuples(res));
                    }
                    return false;
                case PGRES_COMMAND_OK:
                    if (out.ok) out.rows_affected += extract_rows_affected(res);
                    return false;
                case PGRES_COPY_IN:
                case PGRES_COPY_OUT:
                case PGRES_COPY_BOTH:
                    out = QueryResult{};
                    out.ok = false;
                    out.code = PgErrorCode::ProtocolCorrupt;
                    out.error = "COPY cannot run through a query API; use copy_*";
                    out.rows_valid = false;
                    return true;
                default:
                    out = QueryResult{};
                    fill_server_error_fields(res, out);
                    return false;
            }
        }
    } // namespace

    // ---------- lifetime ----------

    PgConnectionLibpq::PgConnectionLibpq() = default;

    PgConnectionLibpq::~PgConnectionLibpq() {
        this->close();
    }

    usub::uvent::task::Awaitable<std::optional<std::string> >
    PgConnectionLibpq::connect_async(const std::string &conninfo, std::chrono::milliseconds timeout) {
        using namespace std::chrono_literals;

        const auto budget = (timeout <= 0ms) ? std::chrono::milliseconds(5s) : timeout;
        const auto start = std::chrono::steady_clock::now();

        auto has_connect_timeout = [](std::string_view ci) {
            size_t i = 0;
            while (i < ci.size()) {
                while (i < ci.size() && (ci[i] == ' ' || ci[i] == '\t')) ++i;
                if (i >= ci.size()) break;

                const size_t key_start = i;
                while (i < ci.size() && ci[i] != '=' && ci[i] != ' ') ++i;
                const std::string_view key(ci.data() + key_start, i - key_start);

                if (i >= ci.size() || ci[i] != '=') break;
                ++i; // skip '='

                if (i < ci.size() && ci[i] == '\'') {
                    ++i;
                    while (i < ci.size()) {
                        if (ci[i] == '\\' && i + 1 < ci.size()) {
                            i += 2;
                            continue;
                        }
                        if (ci[i] == '\'') {
                            ++i;
                            break;
                        }
                        ++i;
                    }
                } else {
                    while (i < ci.size() && ci[i] != ' ' && ci[i] != '\t') ++i;
                }

                if (key == "connect_timeout") return true;
            }
            return false;
        };

        std::string conninfo_with_to = conninfo;
        if (!has_connect_timeout(conninfo_with_to)) {
            auto secs = std::chrono::duration_cast<std::chrono::seconds>(budget).count();
            if (secs <= 0) secs = 1;
            conninfo_with_to += " connect_timeout=" + std::to_string(secs);
        }

        auto fail = [this](std::string err) {
            sock_.reset();
            if (conn_) PQfinish(conn_);
            conn_ = nullptr;
            connected_ = false;
            return std::optional<std::string>{std::move(err)};
        };

        conn_ = PQconnectStart(conninfo_with_to.c_str());
        if (!conn_)
            co_return std::optional<std::string>{"PQconnectStart failed"};

        PQsetNoticeProcessor(conn_, [](void *, const char *) {}, nullptr);

        if (PQstatus(conn_) == CONNECTION_BAD)
            co_return fail(PQerrorMessage(conn_));

        if (PQsetnonblocking(conn_, 1) != 0)
            co_return fail("PQsetnonblocking failed");

        // Handshake: wait for the direction libpq asks for, then poll it
        // again (before the first poll, libpq's contract is "as if it said
        // WRITING"). The deadline timer bounds the whole exchange.
        auto dl = std::make_shared<ConnectDeadline>();
        dl->fd = PQsocket(conn_);
        usub::uvent::system::co_spawn(connect_deadline_timer(dl, budget));

        std::optional<std::string> err;
        PostgresPollingStatusType st = PGRES_POLLING_WRITING;
        while (st != PGRES_POLLING_OK) {
            if (!watch_socket()) {
                err = "PQsocket < 0";
                break;
            }
            if (st == PGRES_POLLING_READING) co_await IoWait{*this, IoWait::Kind::HandshakeRead};
            else co_await IoWait{*this, IoWait::Kind::HandshakeWrite};

            if (detail::cancel_requested()) {
                err = "connect canceled";
                break;
            }
            std::lock_guard<std::mutex> lk(dl->mtx);
            if (dl->fired) {
                const auto elapsed = std::chrono::steady_clock::now() - start;
                err = "connect timeout after " +
                      std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()) + " ms";
                break;
            }
            st = PQconnectPoll(conn_);
            dl->fd = PQsocket(conn_);
            if (st == PGRES_POLLING_FAILED) {
                err = PQerrorMessage(conn_);
                break;
            }
        }
        {
            std::lock_guard<std::mutex> lk(dl->mtx);
            dl->done = true;
            dl->fd = -1;
        }
        if (err) co_return fail(std::move(*err));
        if (!watch_socket()) co_return fail("PQsocket < 0");

        connected_ = true;
        co_return std::nullopt;
    }

    usub::uvent::task::Awaitable<void>
    PgConnectionLibpq::connect_deadline_timer(std::shared_ptr<ConnectDeadline> dl, std::chrono::milliseconds budget) {
        const bool slept = co_await detail::sleep_for(budget);
        if (!slept) co_return;  // canceled: nothing to enforce
        std::lock_guard<std::mutex> lk(dl->mtx);
        if (dl->done) co_return;
        dl->fired = true;
        if (dl->fd >= 0) ::shutdown(dl->fd, SHUT_RDWR);
    }

    bool PgConnectionLibpq::watch_socket() {
        const int fd = conn_ ? PQsocket(conn_) : -1;
        struct stat sb{};
        if (fd < 0 || ::fstat(fd, &sb) != 0) return false;
        const auto ino = static_cast<uint64_t>(sb.st_ino);
        if (sock_ && watched_fd_ == fd && watched_ino_ == ino) return true;

        sock_.reset();
        const int own = ::fcntl(fd, F_DUPFD_CLOEXEC, 0);
        if (own < 0) return false;
        sock_ = std::make_unique<SocketT>(own);
        // libpq reads the outcome of the TCP connect (SO_ERROR) itself; the
        // poller must not consume it first.
        sock_->get_raw_header()->socket_info &=
                static_cast<uint8_t>(~static_cast<uint8_t>(usub::uvent::net::AdditionalState::CONNECTION_PENDING));
        watched_fd_ = fd;
        watched_ino_ = ino;
        return true;
    }

    bool PgConnectionLibpq::connected() const noexcept {
        if (!(connected_ && conn_ && (PQstatus(conn_) == CONNECTION_OK)))
            return false;
        // Cut short by the pool watchdog: the socket is shut down, drop it.
        if (io_timed_out())
            return false;
        // Defensive: if anything else armed the socket timer and it fired,
        // the header is detached from the poller and must not be awaited.
        if (sock_ && (sock_->get_raw_header()->socket_info &
                      static_cast<uint8_t>(usub::uvent::net::AdditionalState::TIMEOUT)))
            return false;
        return true;
    }

    PGconn *PgConnectionLibpq::raw_conn() noexcept { return conn_; }

    bool PgConnectionLibpq::is_idle() {
        if (!connected())
            return false;
        return PQisBusy(conn_) == 0 && PQtransactionStatus(conn_) == PQTRANS_IDLE;
    }

    void PgConnectionLibpq::close() {
        std::lock_guard<std::mutex> lk(close_mtx_);

        UPQ_CONN_DBG("close: conn=%p connected=%d", static_cast<void*>(conn_), connected_ ? 1 : 0);

        connected_ = false;
        if (sock_) sock_->shutdown();
        if (conn_) {
            PQfinish(conn_);
            conn_ = nullptr;
        }
        sock_.reset();
    }

    // ---------- IO deadline bookkeeping ----------

    uint64_t PgConnectionLibpq::steady_now_ms() noexcept {
        using namespace std::chrono;
        return static_cast<uint64_t>(duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count());
    }

    void PgConnectionLibpq::mark_await_begin() noexcept {
        // Publish "an await is parked since now" for the pool watchdog. No
        // socket timer is touched: see the note next to kIoDeadlineMs.
        await_since_ms_.store(steady_now_ms(), std::memory_order_release);
    }

    void PgConnectionLibpq::mark_await_end() noexcept {
        await_since_ms_.store(0, std::memory_order_release);
        // Woken by the watchdog's shutdown(2), not by data: the socket is
        // half-dead, this connection must not be reused.
        if (io_timed_out())
            connected_ = false;
    }

    const char *PgConnectionLibpq::io_error_message() const noexcept {
        if (io_timed_out())
            return io_timeout_detail_.empty() ? "io deadline exceeded" : io_timeout_detail_.c_str();
        const char *m = conn_ ? PQerrorMessage(conn_) : nullptr;
        return m ? m : "";
    }

    bool PgConnectionLibpq::abort_io_from_watchdog() noexcept {
        // Serialised with close(): PQfinish() closes the fd, and a shutdown(2)
        // on an fd number that the kernel has already handed to somebody else
        // would hit a stranger's socket. Under the lock conn_ is either alive
        // (fd valid) or null.
        std::lock_guard<std::mutex> lk(close_mtx_);
        if (!conn_)
            return false;
        // The await may have completed between the watchdog's read of
        // io_await_since_ms() and now; only a still-parked await is cut.
        const uint64_t since = await_since_ms_.load(std::memory_order_acquire);
        if (since == 0)
            return false;
        const int fd = PQsocket(conn_);
        // Diagnostics for the post-mortem: how long the await sat and whether
        // the kernel already holds a reply nobody consumed. Composed BEFORE the
        // release-store below, so the woken coroutine sees it (acquire-load).
        int pending = -1;
        if (fd >= 0 && ::ioctl(fd, FIONREAD, &pending) != 0)
            pending = -1;
        char buf[160];
        std::snprintf(buf, sizeof buf,
                      "io deadline exceeded (waited_ms=%llu deadline_ms=%llu pending_bytes=%d)",
                      static_cast<unsigned long long>(steady_now_ms() - since),
                      static_cast<unsigned long long>(io_deadline_ms()),
                      pending);
        io_timeout_detail_ = buf;
        io_timed_out_.store(true, std::memory_order_release);
        if (fd < 0)
            return false;
        // Wakes the parked coroutine through the owner worker's poller
        // (EPOLLIN|EPOLLHUP|EPOLLRDHUP on an edge-triggered registration is a
        // fresh edge), from whichever thread the watchdog happens to run on.
        ::shutdown(fd, SHUT_RDWR);
        return true;
    }

    // ---------- socket waits ----------

    // A recv() would not block: bytes, EOF or a pending socket error. FIONREAD
    // (SocketHeader::has_unread_bytes) says 0 at EOF, and a FIN that came with
    // the last bytes (a terminated backend: FATAL, then close) shares their
    // readiness edge: libpq reads the bytes, sees the EOF only on its next
    // read, and a wait that ignored EOF would never be woken again.
    static bool read_would_not_block(int fd) noexcept {
        char b;
        const ssize_t n = ::recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n >= 0) return true;
        return errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR;
    }

    bool PgConnectionLibpq::IoWait::await_ready() noexcept {
        auto *hdr = c_.sock_->get_raw_header();
        // After a HUP the poller has dropped (and closed) its registration:
        // nothing would ever wake a park. The next libpq read reports the
        // EOF / error instead.
        if (hdr->fd < 0 || hdr->is_disconnected_now()) return true;

        switch (kind_) {
            case Kind::Read:
            case Kind::ListenRead:
                // Clear the READY hint before probing, never after (see SocketHeader::rd).
                if (hdr->is_read_armed()) hdr->disarm_read();
                if (read_would_not_block(hdr->fd)) return true;
                break;
            case Kind::Write:
                break;
            case Kind::HandshakeRead:
            case Kind::HandshakeWrite: {
                const bool rd = kind_ == Kind::HandshakeRead;
                if (rd && hdr->is_read_armed()) hdr->disarm_read();
                if (!rd && hdr->is_write_armed()) hdr->disarm_write();
                pollfd p{hdr->fd, static_cast<short>(rd ? POLLIN : POLLOUT), 0};
                return ::poll(&p, 1, 0) > 0;  // POLLERR / POLLHUP count as ready
            }
        }
        if (kind_ != Kind::ListenRead) c_.mark_await_begin();
        return false;
    }

    void PgConnectionLibpq::IoWait::await_suspend(std::coroutine_handle<> h) {
        // After the park below another worker may resume (and finish) the
        // coroutine at once: nothing of `this` is touched afterwards.
        auto *hdr = c_.sock_->get_raw_header();
        if (kind_ == Kind::Write || kind_ == Kind::HandshakeWrite)
            usub::uvent::net::detail::AwaiterWrite{hdr}.await_suspend(h);
        else
            usub::uvent::net::detail::AwaiterRead{hdr}.await_suspend(h);
    }

    void PgConnectionLibpq::IoWait::await_resume() noexcept {
        if (kind_ == Kind::Read || kind_ == Kind::Write) c_.mark_await_end();
    }

    // ---------- round-trip steps ----------

    PgConnectionLibpq::Step PgConnectionLibpq::flush_step() {
        if (sock_) {
            auto *hdr = sock_->get_raw_header();
            if (hdr->is_write_armed()) hdr->disarm_write();
        }
        const int fr = PQflush(conn_);
        if (fr == 0) return Step::Done;
        if (fr < 0) {
            connected_ = false;
            return Step::Failed;
        }
        return Step::Wait;
    }

    PgConnectionLibpq::Step PgConnectionLibpq::input_step() {
        if (PQconsumeInput(conn_) == 0) {
            connected_ = false;
            return Step::Failed;
        }
        return PQisBusy(conn_) ? Step::Wait : Step::Done;
    }

    PgConnectionLibpq::Step PgConnectionLibpq::collect_step(QueryResult &out) {
        if (PQconsumeInput(conn_) == 0) {
            connected_ = false;
            return Step::Failed;
        }
        // PQgetResult blocks inside libpq (poll on the socket) while the
        // statement is still executing server-side; only call it when
        // PQisBusy says a result is ready. Otherwise a slow statement (e.g. a
        // contended pg_advisory_xact_lock) parks the whole worker thread.
        while (!PQisBusy(conn_)) {
            PGresult *res = PQgetResult(conn_);
            if (!res) return Step::Done;
            const bool stop = absorb_result(res, out);
            PQclear(res);
            if (stop) {
                connected_ = false;  // stuck in COPY mode: not reusable
                return Step::Done;
            }
        }
        return Step::Wait;
    }

    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::flush_outgoing() {
        for (;;) {
            const Step s = flush_step();
            if (s == Step::Done) co_return true;
            if (s == Step::Failed) co_return false;
            co_await wait_writable();
            if (io_timed_out()) co_return false;
            if (detail::cancel_requested()) {
                connected_ = false;  // reply abandoned mid-protocol
                co_return false;
            }
        }
    }

    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::pump_input() {
        for (;;) {
            const Step s = input_step();
            if (s == Step::Done) co_return true;
            if (s == Step::Failed) co_return false;
            co_await wait_readable();
            if (io_timed_out()) co_return false;
            if (detail::cancel_requested()) {
                connected_ = false;
                co_return false;
            }
        }
    }

    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::round_trip_input() {
        if (!co_await flush_outgoing()) co_return false;
        co_return co_await pump_input();
    }

    bool PgConnectionLibpq::idle_socket_alive() {
        const int fd = PQsocket(conn_);
        if (fd < 0) return false;
        // The poller saw a HUP / RST (and dropped its registration).
        if (!sock_ || sock_->get_raw_header()->fd < 0 || sock_->get_raw_header()->is_disconnected_now())
            return false;
        // Something may have arrived while idle: NOTICE / ParameterStatus, or
        // the FATAL a terminated backend sends right before closing (libpq
        // treats an idle ErrorResponse as a notice, so only the EOF behind it
        // tells). Let libpq read what is there and look again.
        for (int round = 0; round < 4; ++round) {
            char b;
            const ssize_t n = ::recv(fd, &b, 1, MSG_PEEK | MSG_DONTWAIT);
            if (n < 0)
                return (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) &&
                       PQstatus(conn_) == CONNECTION_OK && !PQisBusy(conn_);
            if (n == 0) return false;  // orderly shutdown by the peer
            if (PQconsumeInput(conn_) == 0) {
                connected_ = false;
                return false;
            }
        }
        return PQstatus(conn_) == CONNECTION_OK && !PQisBusy(conn_);
    }

    void PgConnectionLibpq::discard_pending_results() {
        while (!PQisBusy(conn_)) {
            PGresult *res = PQgetResult(conn_);
            if (!res) return;
            PQclear(res);
        }
    }

    // ---------- COPY ----------

    namespace {
        PgCopyResult copy_failure(PgErrorCode code, std::string msg) {
            PgCopyResult r;
            r.ok = false;
            r.code = code;
            r.error = std::move(msg);
            return r;
        }
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_start(const std::string &sql) {
        if (!connected()) co_return copy_failure(PgErrorCode::ConnectionClosed, "connection not OK");

        if (!PQsendQuery(conn_, sql.c_str())) {
            connected_ = false;
            co_return copy_failure(PgErrorCode::SocketReadFailed, PQerrorMessage(conn_));
        }
        if (!co_await round_trip_input()) {
            connected_ = false;
            co_return copy_failure(PgErrorCode::SocketReadFailed, io_error_message());
        }

        PGresult *res = PQgetResult(conn_);
        if (!res) co_return copy_failure(PgErrorCode::ProtocolCorrupt, "no result after COPY start");

        PgCopyResult out;
        if (PQresultStatus(res) == PGRES_COPY_IN) {
            out.ok = true;
            out.code = PgErrorCode::OK;
        } else {
            fill_copy_error(res, out);
        }
        PQclear(res);
        co_return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_send_chunk(const void *data, size_t len) {
        if (!connected()) co_return copy_failure(PgErrorCode::ConnectionClosed, "connection not OK");

        for (;;) {
            const int rc = PQputCopyData(conn_, static_cast<const char *>(data), static_cast<int>(len));
            if (rc == 1) break;
            if (rc < 0) {
                connected_ = false;
                co_return copy_failure(PgErrorCode::SocketReadFailed, PQerrorMessage(conn_));
            }
            if (!co_await flush_outgoing()) {
                connected_ = false;
                co_return copy_failure(PgErrorCode::SocketReadFailed, io_error_message());
            }
        }

        PgCopyResult out;
        out.ok = true;
        out.code = PgErrorCode::OK;
        co_return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_finish(const char *error_msg) {
        if (!connected()) co_return copy_failure(PgErrorCode::ConnectionClosed, "connection not OK");

        for (;;) {
            const int rc = PQputCopyEnd(conn_, error_msg);
            if (rc == 1) break;
            if (rc < 0) {
                connected_ = false;
                co_return copy_failure(PgErrorCode::SocketReadFailed, PQerrorMessage(conn_));
            }
            if (!co_await flush_outgoing()) {
                connected_ = false;
                co_return copy_failure(PgErrorCode::SocketReadFailed, io_error_message());
            }
        }

        if (!co_await round_trip_input()) {
            connected_ = false;
            co_return copy_failure(PgErrorCode::SocketReadFailed, io_error_message());
        }
        co_return drain_copy_end_result();
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_out_start(const std::string &sql) {
        if (!connected()) co_return copy_failure(PgErrorCode::ConnectionClosed, "connection not OK");

        if (!PQsendQuery(conn_, sql.c_str())) {
            connected_ = false;
            co_return copy_failure(PgErrorCode::SocketReadFailed, PQerrorMessage(conn_));
        }
        if (!co_await round_trip_input()) {
            connected_ = false;
            co_return copy_failure(PgErrorCode::SocketReadFailed, io_error_message());
        }

        PGresult *res = PQgetResult(conn_);
        if (!res) co_return copy_failure(PgErrorCode::ProtocolCorrupt, "no result after COPY start");

        PgCopyResult out;
        if (PQresultStatus(res) == PGRES_COPY_OUT) {
            out.ok = true;
            out.code = PgErrorCode::OK;
        } else {
            fill_copy_error(res, out);
        }
        PQclear(res);
        co_return out;
    }

    usub::uvent::task::Awaitable<PgWireResult<std::vector<uint8_t> > >
    PgConnectionLibpq::copy_out_read_chunk() {
        PgWireResult<std::vector<uint8_t> > out{};
        out.ok = false;

        auto fail = [&](PgErrorCode code, std::string msg) {
            out.ok = false;
            out.err.code = code;
            out.err.message = std::move(msg);
            return std::move(out);
        };

        if (!connected()) co_return fail(PgErrorCode::ConnectionClosed, "connection not OK");

        for (;;) {
            char *buf = nullptr;
            const int rc = PQgetCopyData(conn_, &buf, /*async=*/1);

            if (rc > 0) {
                out.value.assign(reinterpret_cast<const uint8_t *>(buf), reinterpret_cast<const uint8_t *>(buf) + rc);
                PQfreemem(buf);
                out.ok = true;
                co_return std::move(out);
            }

            if (rc == 0) {  // nothing buffered yet
                co_await wait_readable();
                if (io_timed_out()) co_return fail(PgErrorCode::SocketReadFailed, io_error_message());
                if (detail::cancel_requested()) {
                    connected_ = false;
                    co_return fail(PgErrorCode::AwaitCanceled, "canceled");
                }
                if (PQconsumeInput(conn_) == 0) {
                    connected_ = false;
                    co_return fail(PgErrorCode::SocketReadFailed, PQerrorMessage(conn_));
                }
                continue;
            }

            if (rc == -1) {  // end of COPY OUT: collect the command result
                PGresult *res = PQgetResult(conn_);
                if (!res) co_return fail(PgErrorCode::ProtocolCorrupt, "COPY OUT ended but no result");

                const auto st = PQresultStatus(res);
                PgCopyResult err{};
                const bool ok = st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK;
                if (!ok) fill_copy_error(res, err);
                PQclear(res);
                while (PGresult *extra = PQgetResult(conn_)) PQclear(extra);

                if (!ok) co_return fail(err.code, std::move(err.error));
                out.ok = true;
                out.value.clear();
                co_return std::move(out);
            }

            connected_ = false;
            co_return fail(PgErrorCode::SocketReadFailed, PQerrorMessage(conn_));
        }
    }

    PgCopyResult PgConnectionLibpq::drain_copy_end_result() {
        PgCopyResult out;
        out.ok = true;
        out.code = PgErrorCode::OK;

        while (!PQisBusy(conn_)) {
            PGresult *res = PQgetResult(conn_);
            if (!res) break;
            if (PQresultStatus(res) == PGRES_COMMAND_OK) {
                if (out.ok) out.rows_affected += extract_rows_affected(res);
            } else {
                out = PgCopyResult{};
                fill_copy_error(res, out);
            }
            PQclear(res);
        }
        return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_from_buffer(const std::string &sql, const void *data, size_t len) {
        PgCopyResult start = co_await copy_in_start(sql);
        if (!start.ok) co_return start;

        if (len > 0) {
            PgCopyResult send = co_await copy_in_send_chunk(data, len);
            if (!send.ok) {
                co_await copy_in_finish("aborted: send chunk failed");
                co_return send;
            }
        }
        co_return co_await copy_in_finish();
    }

    // ---------- snapshots ----------

    usub::uvent::task::Awaitable<std::expected<std::string, PgOpError> >
    PgConnectionLibpq::export_snapshot() {
        return run_query(detail::SimpleSend<std::string>{"SELECT pg_export_snapshot();"},
                         [](QueryResult &&qr) -> std::expected<std::string, PgOpError> {
                             if (!qr.ok) return std::unexpected(detail::to_op_error(qr));
                             if (qr.rows.empty() || qr.rows[0].cols.empty())
                                 return std::unexpected(PgOpError{PgErrorCode::ProtocolCorrupt,
                                                                  "pg_export_snapshot returned no value", {}});
                             return std::move(qr.rows[0].cols[0]);
                         });
    }

    usub::uvent::task::Awaitable<std::optional<PgOpError> >
    PgConnectionLibpq::set_snapshot(const std::string &snapshot_id) {
        for (const char c : snapshot_id) {
            const auto u = static_cast<unsigned char>(c);
            const bool ok = (u >= '0' && u <= '9') || (u >= 'a' && u <= 'f') || (u >= 'A' && u <= 'F') || u == '-';
            if (!ok)
                return detail::ready(std::make_optional(
                    PgOpError{PgErrorCode::ProtocolCorrupt, "invalid snapshot id format", {}}));
        }
        return run_query(detail::SimpleSend<std::string>{"SET TRANSACTION SNAPSHOT '" + snapshot_id + "';"},
                         [](QueryResult &&qr) -> std::optional<PgOpError> {
                             if (!qr.ok) return detail::to_op_error(qr);
                             return std::nullopt;
                         });
    }

    // ---------- cursors ----------

    bool PgConnectionLibpq::is_safe_cursor_ident(const std::string &s) {
        if (s.empty() || s.size() > 63) return false;
        const auto c0 = static_cast<unsigned char>(s[0]);
        if (!((c0 >= 'a' && c0 <= 'z') || (c0 >= 'A' && c0 <= 'Z') || c0 == '_'))
            return false;
        for (size_t i = 1; i < s.size(); ++i) {
            const auto c = static_cast<unsigned char>(s[i]);
            const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
            if (!ok) return false;
        }
        return true;
    }

    std::string PgConnectionLibpq::make_cursor_name() {
        return "usub_cur_" + std::to_string(++cursor_seq_);
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_declare_in_tx(const std::string &cursor_name, const std::string &sql) {
        if (!is_safe_cursor_ident(cursor_name)) return detail::ready(detail::invalid_cursor_result());
        return run_query(detail::SimpleSend<std::string>{"DECLARE " + cursor_name + " NO SCROLL CURSOR FOR " + sql + ";"},
                         detail::AsResult{});
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_declare(const std::string &cursor_name, const std::string &sql) {
        if (!is_safe_cursor_ident(cursor_name)) return detail::ready(detail::invalid_cursor_result());
        return run_query(
            detail::SimpleSend<std::string>{"BEGIN; DECLARE " + cursor_name + " NO SCROLL CURSOR FOR " + sql + ";"},
            detail::AsResult{});
    }

    usub::uvent::task::Awaitable<PgCursorChunk>
    PgConnectionLibpq::cursor_fetch_chunk(const std::string &cursor_name, uint32_t count) {
        if (!is_safe_cursor_ident(cursor_name)) {
            PgCursorChunk bad{};
            bad.code = PgErrorCode::ProtocolCorrupt;
            bad.error = "invalid cursor name";
            return detail::ready(std::move(bad));
        }
        return run_query(
            detail::SimpleSend<std::string>{"FETCH FORWARD " + std::to_string(count) + " FROM " + cursor_name + ";"},
            [](QueryResult &&qr) {
                PgCursorChunk chunk{};
                chunk.ok = qr.ok;
                chunk.code = qr.code;
                if (!qr.ok) {
                    chunk.error = std::move(qr.error);
                    chunk.err_detail = std::move(qr.err_detail);
                    chunk.done = true;
                    return chunk;
                }
                chunk.rows = std::move(qr.rows);
                chunk.columns = std::move(qr.columns);
                chunk.done = chunk.rows.empty();
                return chunk;
            });
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_close_in_tx(const std::string &cursor_name) {
        if (!is_safe_cursor_ident(cursor_name)) return detail::ready(detail::invalid_cursor_result());
        return run_query(detail::SimpleSend<std::string>{"CLOSE " + cursor_name + ";"}, detail::AsResult{});
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_close(const std::string &cursor_name) {
        if (!is_safe_cursor_ident(cursor_name)) return detail::ready(detail::invalid_cursor_result());
        return run_query(detail::SimpleSend<std::string>{"CLOSE " + cursor_name + "; COMMIT;"},
                         [](QueryResult &&qr) {
                             qr.rows.clear();
                             qr.rows_valid = true;
                             return std::move(qr);
                         });
    }
} // namespace usub::pg
