#include "upq/PgConnection.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace usub::pg {
    static void fill_server_error_fields_copy(PGresult *res, PgCopyResult &out) {
        if (!res) return;
        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char *primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char *detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char *hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char *fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        out.ok = false;
        out.code = PgErrorCode::ServerError;

        if (sqlstate && *sqlstate) { out.error.append(" [SQLSTATE ").append(sqlstate).append("]"); }
        if (detail && *detail) { out.error.append(" detail: ").append(detail); }
        if (hint && *hint) { out.error.append(" hint: ").append(hint); }
    }

    static void fill_server_error_fields_cursor(PGresult *res, PgCursorChunk &out) {
        if (!res) return;
        const char *sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        const char *primary = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
        const char *detail = PQresultErrorField(res, PG_DIAG_MESSAGE_DETAIL);
        const char *hint = PQresultErrorField(res, PG_DIAG_MESSAGE_HINT);

        if (primary && *primary) out.error = primary;
        else if (const char *fb = PQresultErrorMessage(res); fb && *fb) out.error = fb;

        out.ok = false;
        out.code = PgErrorCode::ServerError;
        out.done = true;

        if (sqlstate && *sqlstate) { out.error.append(" [SQLSTATE ").append(sqlstate).append("]"); }
        if (detail && *detail) { out.error.append(" detail: ").append(detail); }
        if (hint && *hint) { out.error.append(" hint: ").append(hint); }
    }

    PgConnectionLibpq::PgConnectionLibpq() = default;

    PgConnectionLibpq::~PgConnectionLibpq() {
        this->close();
    }

    usub::uvent::task::Awaitable<std::optional<std::string> >
    PgConnectionLibpq::connect_async(const std::string &conninfo) {
        using namespace std::chrono_literals;
        co_return co_await connect_async(conninfo, 5s);
    }

    usub::uvent::task::Awaitable<std::optional<std::string> >
    PgConnectionLibpq::connect_async(const std::string &conninfo,
                                     std::chrono::milliseconds timeout) {
        using namespace std::chrono_literals;

        auto clamped = (timeout <= 0ms) ? 5s : timeout;
        const auto start = std::chrono::steady_clock::now();
        const auto deadline = start + clamped;

        auto has_connect_timeout = [](std::string_view ci) {
            size_t i = 0;
            while (i < ci.size()) {
                while (i < ci.size() && (ci[i] == ' ' || ci[i] == '\t')) ++i;
                if (i >= ci.size()) break;

                size_t key_start = i;
                while (i < ci.size() && ci[i] != '=' && ci[i] != ' ') ++i;
                std::string_view key(ci.data() + key_start, i - key_start);

                if (i >= ci.size() || ci[i] != '=') break;
                ++i; // skip '='

                if (i < ci.size() && ci[i] == '\'') {
                    ++i;
                    while (i < ci.size()) {
                        if (ci[i] == '\\' && i + 1 < ci.size()) { i += 2; continue; }
                        if (ci[i] == '\'') { ++i; break; }
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
            auto secs = std::chrono::duration_cast<std::chrono::seconds>(clamped).count();
            if (secs <= 0) secs = 1;
            conninfo_with_to += " connect_timeout=" + std::to_string(secs);
        }

        conn_ = PQconnectStart(conninfo_with_to.c_str());
        if (!conn_)
            co_return std::optional<std::string>{"PQconnectStart failed"};

        PQsetNoticeProcessor(
            conn_,
            [](void*, const char*) {},
            nullptr);

        if (PQstatus(conn_) == CONNECTION_BAD) {
            std::string err = PQerrorMessage(conn_);
            PQfinish(conn_);
            conn_ = nullptr;
            co_return std::optional<std::string>{std::move(err)};
        }

        if (PQsetnonblocking(conn_, 1) != 0) {
            std::string err = "PQsetnonblocking failed";
            PQfinish(conn_);
            conn_ = nullptr;
            co_return std::optional<std::string>{std::move(err)};
        }

        for (;;) {
            auto st = PQconnectPoll(conn_);
            if (st == PGRES_POLLING_OK) {
                break;
            }

            if (st == PGRES_POLLING_FAILED) {
                std::string err = PQerrorMessage(conn_);
                PQfinish(conn_);
                conn_ = nullptr;
                connected_ = false;
                co_return std::optional<std::string>{std::move(err)};
            }

            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                auto elapsed_ms =
                        std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
                std::string err = "connect timeout after " + std::to_string(elapsed_ms) + " ms";
                PQfinish(conn_);
                conn_ = nullptr;
                connected_ = false;
                co_return std::optional<std::string>{std::move(err)};
            }

            co_await usub::uvent::system::this_coroutine::sleep_for(5ms);
        }

        const int fd = PQsocket(conn_);
        if (fd < 0) {
            std::string err = "PQsocket < 0";
            PQfinish(conn_);
            conn_ = nullptr;
            connected_ = false;
            co_return std::optional<std::string>{std::move(err)};
        }

        sock_ = std::make_unique<
            usub::uvent::net::Socket<
                usub::uvent::net::Proto::TCP,
                usub::uvent::net::Role::ACTIVE
            >
        >(fd);

        connected_ = true;
        co_return std::nullopt;
    }

    bool PgConnectionLibpq::connected() const noexcept {
        return connected_ && conn_ && (PQstatus(conn_) == CONNECTION_OK);
    }

    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::flush_outgoing() {
        for (;;) {
            const int fr = PQflush(conn_);
            if (fr == 0) co_return true;
            if (fr == -1) {
                connected_ = false;
                co_return false;
            }
            co_await wait_writable();
        }
    }

    usub::uvent::task::Awaitable<bool> PgConnectionLibpq::pump_input() {
        for (;;) {
            if (PQconsumeInput(conn_) == 0) {
                connected_ = false;
                co_return false;
            }
            if (!PQisBusy(conn_)) co_return true;
            co_await wait_readable();
        }
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_readable() {
        co_await usub::uvent::net::detail::AwaiterRead{sock_->get_raw_header()};
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_writable() {
        co_await usub::uvent::net::detail::AwaiterWrite{sock_->get_raw_header()};
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgConnectionLibpq::wait_readable_for_listener() {
        co_await wait_readable();
        co_return;
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::exec_simple_query_nonblocking(const std::string &sql) {
        QueryResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.rows_valid = true;
        out.rows_affected = 0;

        if (!connected()) {
            out.ok = false;
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            out.rows_valid = false;
            co_return out;
        }

        if (!PQsendQuery(conn_, sql.c_str())) {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            out.rows_valid = false;
            connected_ = false;
            co_return out;
        }

        if (!(co_await flush_outgoing())) {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            out.rows_valid = false;
            connected_ = false;
            co_return out;
        }

        if (!(co_await pump_input())) {
            out.ok = false;
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            out.rows_valid = false;
            connected_ = false;
            co_return out;
        }

        co_return drain_all_results();
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_start(const std::string &sql) {
        PgCopyResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;

        if (!connected()) {
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        if (!PQsendQuery(conn_, sql.c_str())) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        if (!(co_await flush_outgoing())) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        if (!(co_await pump_input())) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        PGresult *res = PQgetResult(conn_);
        if (!res) {
            out.code = PgErrorCode::ProtocolCorrupt;
            out.error = "no result after COPY start";
            co_return out;
        }

        if (PQresultStatus(res) != PGRES_COPY_IN) {
            fill_server_error_fields_copy(res, out);
            PQclear(res);
            co_return out;
        }

        out.ok = true;
        out.code = PgErrorCode::OK;
        PQclear(res);
        co_return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_send_chunk(const void *data, size_t len) {
        PgCopyResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;

        if (!connected()) {
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        for (;;) {
            const int rc = PQputCopyData(conn_,
                                         reinterpret_cast<const char *>(data),
                                         static_cast<int>(len));
            if (rc == 1) break;
            if (rc < 0) {
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
                connected_ = false;
                co_return out;
            }
            if (!(co_await flush_outgoing())) {
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
                connected_ = false;
                co_return out;
            }
        }

        out.ok = true;
        out.code = PgErrorCode::OK;
        co_return out;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_finish() {
        co_return co_await copy_in_finish(nullptr);
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_in_finish(const char *error_msg) {
        PgCopyResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.rows_affected = 0;

        if (!connected()) {
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        for (;;) {
            const int rc = PQputCopyEnd(conn_, error_msg);
            if (rc == 1) break;
            if (rc < 0) {
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
                connected_ = false;
                co_return out;
            }
            if (!(co_await flush_outgoing())) {
                out.code = PgErrorCode::SocketReadFailed;
                out.error = PQerrorMessage(conn_);
                connected_ = false;
                co_return out;
            }
        }

        if (!(co_await flush_outgoing())) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        if (!(co_await pump_input())) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        co_return drain_copy_end_result();
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_out_start(const std::string &sql) {
        PgCopyResult out{};
        out.ok = false;
        out.code = PgErrorCode::Unknown;
        out.rows_affected = 0;

        if (!connected()) {
            out.code = PgErrorCode::ConnectionClosed;
            out.error = "connection not OK";
            co_return out;
        }

        if (!PQsendQuery(conn_, sql.c_str())) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        if (!(co_await flush_outgoing())) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        if (!(co_await pump_input())) {
            out.code = PgErrorCode::SocketReadFailed;
            out.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }

        PGresult *res = PQgetResult(conn_);
        if (!res) {
            out.code = PgErrorCode::ProtocolCorrupt;
            out.error = "no result after COPY start";
            co_return out;
        }

        if (PQresultStatus(res) != PGRES_COPY_OUT) {
            fill_server_error_fields_copy(res, out);
            PQclear(res);
            co_return out;
        }

        out.ok = true;
        out.code = PgErrorCode::OK;
        PQclear(res);
        co_return out;
    }

    usub::uvent::task::Awaitable<PgWireResult<std::vector<uint8_t> > >
    PgConnectionLibpq::copy_out_read_chunk() {
        PgWireResult<std::vector<uint8_t> > out{};
        out.ok = false;
        out.err.code = PgErrorCode::Unknown;

        if (!connected()) {
            out.ok = false;
            out.err.code = PgErrorCode::ConnectionClosed;
            out.err.message = "connection not OK";
            co_return out;
        }

        for (;;) {
            char *buf = nullptr;
            const int rc = PQgetCopyData(conn_, &buf, /*async=*/1);

            if (rc > 0) {
                out.value.resize(static_cast<size_t>(rc));
                std::memcpy(out.value.data(), buf, static_cast<size_t>(rc));
                PQfreemem(buf);
                out.ok = true;
                co_return out;
            }

            if (rc == 0) {
                co_await wait_readable();
                if (PQconsumeInput(conn_) == 0) {
                    out.ok = false;
                    out.err.code = PgErrorCode::SocketReadFailed;
                    out.err.message = PQerrorMessage(conn_);
                    connected_ = false;
                    co_return out;
                }
                continue;
            }

            if (rc == -1) {
                if (PGresult *res = PQgetResult(conn_)) {
                    const auto st = PQresultStatus(res);
                    if (st == PGRES_COMMAND_OK || st == PGRES_TUPLES_OK) {
                        PQclear(res);
                        while (PGresult *extra = PQgetResult(conn_))
                            PQclear(extra);
                        out.ok = true;
                        out.value.clear();
                        co_return out;
                    }
                    PgCopyResult tmp_err{};
                    fill_server_error_fields_copy(res, tmp_err);
                    PQclear(res);
                    while (PGresult *extra = PQgetResult(conn_))
                        PQclear(extra);
                    out.ok = false;
                    out.err.code = tmp_err.code;
                    out.err.message = tmp_err.error;
                    co_return out;
                }
                out.ok = false;
                out.err.code = PgErrorCode::ProtocolCorrupt;
                out.err.message = "COPY OUT ended but no result";
                co_return out;
            }

            out.ok = false;
            out.err.code = PgErrorCode::SocketReadFailed;
            out.err.message = PQerrorMessage(conn_);
            connected_ = false;
            co_return out;
        }
    }

    bool PgConnectionLibpq::is_safe_cursor_ident(const std::string &s) {
        if (s.empty() || s.size() > 63) return false;
        auto c0 = static_cast<unsigned char>(s[0]);
        if (!((c0 >= 'a' && c0 <= 'z') || (c0 >= 'A' && c0 <= 'Z') || c0 == '_'))
            return false;
        for (size_t i = 1; i < s.size(); ++i) {
            unsigned char c = static_cast<unsigned char>(s[i]);
            bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                      (c >= '0' && c <= '9') || c == '_';
            if (!ok) return false;
        }
        return true;
    }

    std::string PgConnectionLibpq::make_cursor_name() {
        const uint64_t seq = ++cursor_seq_;
        char buf[64];
        std::snprintf(buf, sizeof(buf), "usub_cur_%llu", static_cast<unsigned long long>(seq));
        return std::string(buf);
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_declare_in_tx(const std::string &cursor_name,
                                            const std::string &sql) {
        if (!is_safe_cursor_ident(cursor_name)) {
            QueryResult err{};
            err.ok = false;
            err.code = PgErrorCode::ProtocolCorrupt;
            err.error = "invalid cursor name";
            err.rows_valid = false;
            co_return err;
        }
        const std::string stmt = "DECLARE " + cursor_name + " NO SCROLL CURSOR FOR " + sql + ";";
        co_return co_await exec_simple_query_nonblocking(stmt);
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_declare(const std::string &cursor_name, const std::string &sql) {
        if (!is_safe_cursor_ident(cursor_name)) {
            QueryResult err{};
            err.ok = false;
            err.code = PgErrorCode::ProtocolCorrupt;
            err.error = "invalid cursor name";
            err.rows_valid = false;
            co_return err;
        }
        std::string full = "BEGIN; DECLARE " + cursor_name + " NO SCROLL CURSOR FOR " + sql + ";";
        co_return co_await exec_simple_query_nonblocking(full);
    }

    usub::uvent::task::Awaitable<PgCursorChunk>
    PgConnectionLibpq::cursor_fetch_chunk(const std::string &cursor_name, uint32_t count) {
        PgCursorChunk chunk{};
        chunk.ok = false;
        chunk.code = PgErrorCode::Unknown;

        if (!is_safe_cursor_ident(cursor_name)) {
            chunk.code = PgErrorCode::ProtocolCorrupt;
            chunk.error = "invalid cursor name";
            co_return chunk;
        }

        if (!connected()) {
            chunk.code = PgErrorCode::ConnectionClosed;
            chunk.error = "connection not OK";
            co_return chunk;
        }

        char cntbuf[32];
        std::snprintf(cntbuf, sizeof(cntbuf), "%u", count);
        const std::string fetch_sql = "FETCH FORWARD " + std::string(cntbuf) + " FROM " + cursor_name + ";";

        if (!PQsendQuery(conn_, fetch_sql.c_str())) {
            chunk.code = PgErrorCode::SocketReadFailed;
            chunk.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return chunk;
        }

        if (!(co_await flush_outgoing())) {
            chunk.code = PgErrorCode::SocketReadFailed;
            chunk.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return chunk;
        }

        if (!(co_await pump_input())) {
            chunk.code = PgErrorCode::SocketReadFailed;
            chunk.error = PQerrorMessage(conn_);
            connected_ = false;
            co_return chunk;
        }

        PgCursorChunk out = drain_single_result_rows();

        for (;;) {
            if (!(co_await pump_input())) {
                chunk.code = PgErrorCode::SocketReadFailed;
                chunk.error = PQerrorMessage(conn_);
                connected_ = false;
                co_return chunk;
            }
            PGresult *leftover = PQgetResult(conn_);
            if (!leftover) break;
            const auto st = PQresultStatus(leftover);
            if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
                fill_server_error_fields(leftover, reinterpret_cast<QueryResult &>(out));
                out.ok = false;
                out.done = true;
            }
            PQclear(leftover);
        }

        co_return out;
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_close_in_tx(const std::string &cursor_name) {
        QueryResult final{};
        final.ok = false;
        final.code = PgErrorCode::Unknown;
        final.rows_valid = true;
        final.rows_affected = 0;

        if (!is_safe_cursor_ident(cursor_name)) {
            final.code = PgErrorCode::ProtocolCorrupt;
            final.error = "invalid cursor name";
            final.rows_valid = false;
            co_return final;
        }
        co_return co_await exec_simple_query_nonblocking("CLOSE " + cursor_name + ";");
    }

    usub::uvent::task::Awaitable<QueryResult>
    PgConnectionLibpq::cursor_close(const std::string &cursor_name) {
        QueryResult final{};
        final.ok = false;
        final.code = PgErrorCode::Unknown;
        final.rows_valid = true;
        final.rows_affected = 0;

        if (!is_safe_cursor_ident(cursor_name)) {
            final.code = PgErrorCode::ProtocolCorrupt;
            final.error = "invalid cursor name";
            final.rows_valid = false;
            co_return final;
        }

        if (!connected()) {
            final.code = PgErrorCode::ConnectionClosed;
            final.error = "connection not OK";
            final.rows_valid = false;
            co_return final;
        }

        const std::string close_sql = "CLOSE " + cursor_name + "; COMMIT;";
        if (!PQsendQuery(conn_, close_sql.c_str())) {
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(conn_);
            final.rows_valid = false;
            connected_ = false;
            co_return final;
        }

        if (!(co_await flush_outgoing())) {
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(conn_);
            final.rows_valid = false;
            connected_ = false;
            co_return final;
        }

        if (!(co_await pump_input())) {
            final.code = PgErrorCode::SocketReadFailed;
            final.error = PQerrorMessage(conn_);
            final.rows_valid = false;
            connected_ = false;
            co_return final;
        }

        final = drain_all_results();
        final.rows.clear();
        final.rows_valid = true;
        co_return final;
    }

    PGconn *PgConnectionLibpq::raw_conn() noexcept { return conn_; }

    bool PgConnectionLibpq::is_idle() {
        if (!connected())
            return false;

        return PQisBusy(this->conn_) == 0 &&
               PQtransactionStatus(this->conn_) == PQTRANS_IDLE;
    }

    void PgConnectionLibpq::close() {
        std::lock_guard<std::mutex> lk(close_mtx_);

        UPQ_CONN_DBG("close: conn=%p connected=%d", static_cast<void*>(conn_), connected_ ? 1 : 0);

        this->connected_ = false;

        if (this->sock_) {
            this->sock_->shutdown();
        }

        if (this->conn_) {
            PQfinish(this->conn_);
            this->conn_ = nullptr;
        }

        this->sock_.reset();
    }

    usub::pg::QueryResult PgConnectionLibpq::drain_all_results() {
        QueryResult final_out;
        final_out.ok = true;
        final_out.code = PgErrorCode::OK;
        final_out.rows_valid = true;
        final_out.rows_affected = 0;
        final_out.columns.clear();

        while (PGresult *res = PQgetResult(conn_)) {
            QueryResult tmp;
            const auto st = PQresultStatus(res);
            if (st == PGRES_TUPLES_OK) {
                const int nrows = PQntuples(res);
                const int ncols = PQnfields(res);

                tmp.columns.reserve(ncols);
                for (int c = 0; c < ncols; ++c) {
                    const char *nm = PQfname(res, c);
                    tmp.columns.emplace_back(nm ? nm : "");
                }

#if UPQ_REFLECT_DEBUG
                {
                    std::string cols_line;
                    cols_line.reserve(64);
                    for (int c = 0; c < ncols; ++c) {
                        if (c) cols_line += ", ";
                        cols_line += tmp.columns[c];
                    }
                }
#endif

                for (int r = 0; r < nrows; ++r) {
                    QueryResult::Row row;
                    row.cols.reserve(ncols);
                    for (int c = 0; c < ncols; ++c) {
                        if (PQgetisnull(res, r, c)) {
                            row.cols.emplace_back();
                        } else {
                            const char *v = PQgetvalue(res, r, c);
                            const int len = PQgetlength(res, r, c);
                            row.cols.emplace_back(v, static_cast<size_t>(len));
                        }
                    }
                    tmp.rows.emplace_back(std::move(row));
                }

                tmp.ok = true;
                tmp.code = PgErrorCode::OK;
                if (tmp.rows_affected == 0)
                    tmp.rows_affected = static_cast<uint64_t>(tmp.rows.size());
            } else if (st == PGRES_COMMAND_OK) {
                tmp.ok = true;
                tmp.code = PgErrorCode::OK;
                tmp.rows_affected = extract_rows_affected(res);
            } else {
                fill_server_error_fields(res, tmp);
            }
            PQclear(res);

            if (!tmp.ok) {
                final_out = std::move(tmp);
            } else {
                final_out.rows_affected += tmp.rows_affected;

                if (final_out.columns.empty() && !tmp.columns.empty())
                    final_out.columns = tmp.columns;

                if (tmp.rows_valid && !tmp.rows.empty()) {
                    final_out.rows.reserve(final_out.rows.size() + tmp.rows.size());
                    for (auto &r: tmp.rows)
                        final_out.rows.emplace_back(std::move(r));
                }
            }
        }

        if (!final_out.ok) final_out.rows_valid = false;
        return final_out;
    }

    PgCopyResult PgConnectionLibpq::drain_copy_end_result() {
        PgCopyResult out;
        out.ok = true;
        out.code = PgErrorCode::OK;
        out.rows_affected = 0;

        while (PGresult *res = PQgetResult(conn_)) {
            PgCopyResult tmp{};
            if (PQresultStatus(res) == PGRES_COMMAND_OK) {
                tmp.ok = true;
                tmp.code = PgErrorCode::OK;
                if (const char *aff = PQcmdTuples(res); aff && *aff)
                    tmp.rows_affected = std::strtoull(aff, nullptr, 10);
            } else {
                fill_server_error_fields(res, reinterpret_cast<QueryResult &>(tmp));
            }
            PQclear(res);

            if (!tmp.ok) out = std::move(tmp);
            else out.rows_affected += tmp.rows_affected;
        }
        return out;
    }

    PgCursorChunk PgConnectionLibpq::drain_single_result_rows() {
        if (PGresult *res = PQgetResult(conn_)) {
            PgCursorChunk out{};
            const auto st = PQresultStatus(res);
            if (st == PGRES_TUPLES_OK) {
                const int nrows = PQntuples(res);
                const int ncols = PQnfields(res);
                out.columns.reserve(ncols);
                for (int c = 0; c < ncols; ++c) {
                    const char *nm = PQfname(res, c);
                    out.columns.emplace_back(nm ? nm : "");
                }
                out.rows.reserve(nrows);
                for (int r = 0; r < nrows; ++r) {
                    QueryResult::Row row;
                    row.cols.reserve(ncols);
                    for (int c = 0; c < ncols; ++c) {
                        if (PQgetisnull(res, r, c)) row.cols.emplace_back();
                        else {
                            const char *v = PQgetvalue(res, r, c);
                            const int len = PQgetlength(res, r, c);
                            row.cols.emplace_back(v, static_cast<size_t>(len));
                        }
                    }
                    out.rows.emplace_back(std::move(row));
                }
                out.ok = true;
                out.code = PgErrorCode::OK;
                out.done = (nrows == 0);
            } else if (st == PGRES_COMMAND_OK) {
                out.ok = true;
                out.code = PgErrorCode::OK;
                out.done = true;
            } else {
                fill_server_error_fields(res, reinterpret_cast<QueryResult &>(out));
                out.done = true;
            }
            PQclear(res);
            return out;
        }

        PgCursorChunk ok{};
        ok.ok = true;
        ok.code = PgErrorCode::OK;
        ok.done = true;
        return ok;
    }

    usub::uvent::task::Awaitable<std::expected<std::string, PgOpError> >
    PgConnectionLibpq::export_snapshot() {
        QueryResult qr = co_await exec_simple_query_nonblocking(
            "SELECT pg_export_snapshot();");
        if (!qr.ok)
            co_return std::unexpected(PgOpError{qr.code, qr.error, qr.err_detail});
        if (qr.rows.empty() || qr.rows[0].cols.empty())
            co_return std::unexpected(
                PgOpError{PgErrorCode::ProtocolCorrupt,
                          "pg_export_snapshot returned no value", {}});
        co_return std::expected<std::string, PgOpError>{
            std::in_place, qr.rows[0].cols[0]};
    }

    usub::uvent::task::Awaitable<std::optional<PgOpError> >
    PgConnectionLibpq::set_snapshot(const std::string &snapshot_id) {
        for (char c : snapshot_id) {
            const unsigned char u = static_cast<unsigned char>(c);
            const bool ok =
                (u >= '0' && u <= '9') || (u >= 'a' && u <= 'f') ||
                (u >= 'A' && u <= 'F') || u == '-';
            if (!ok) {
                co_return std::make_optional(PgOpError{
                    PgErrorCode::ProtocolCorrupt,
                    "invalid snapshot id format", {}});
            }
        }
        const std::string sql =
            "SET TRANSACTION SNAPSHOT '" + snapshot_id + "';";
        QueryResult qr = co_await exec_simple_query_nonblocking(sql);
        if (!qr.ok)
            co_return std::make_optional(PgOpError{qr.code, qr.error, qr.err_detail});
        co_return std::nullopt;
    }

    usub::uvent::task::Awaitable<PgCopyResult>
    PgConnectionLibpq::copy_from_buffer(const std::string &sql,
                                        const void *data, size_t len) {
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
} // namespace usub::pg