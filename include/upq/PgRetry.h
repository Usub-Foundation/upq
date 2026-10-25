#ifndef PGRETRY_H
#define PGRETRY_H

#include <cstdint>
#include <functional>
#include <string_view>

#include "PgTypes.h"

namespace usub::pg {
    // What a failed statement tells about its own fate. The pool, transactions
    // and run_tx() decide whether to re-run it from this class alone, so every
    // service sees the same answer for the same error.
    enum class RetryClass : uint8_t {
        // Not retryable: success, a server ERROR caused by the statement itself
        // (constraint, syntax, no rows, ...), cancellation, misuse.
        None = 0,
        // The statement provably never reached a backend: the pooler could not
        // hand it to a server connection (08P01 "server login has been
        // failing", "query_wait_timeout", ...), auth relayed by the pooler
        // (28P01), too_many_connections (53300), cannot_connect_now (57P03).
        // Safe to repeat even for writes.
        NotStarted,
        // The connection died under the statement (EOF/RST, admin/crash
        // shutdown, idle-in-transaction kill, libpq "server closed the
        // connection"). The server may or may not have applied it.
        ConnectionLost,
        // Our IO deadline cut the await (PgConnectionLibpq::io_timed_out): the
        // statement may still be running server-side.
        Deadline,
        // The server rolled the statement back: serialization failure (40001),
        // deadlock (40P01), lock timeout (55P03). It had no effect, but inside
        // an explicit transaction the whole transaction is gone.
        RolledBack,
    };

    inline const char *toString(RetryClass c) noexcept {
        switch (c) {
            case RetryClass::None: return "none";
            case RetryClass::NotStarted: return "not_started";
            case RetryClass::ConnectionLost: return "connection_lost";
            case RetryClass::Deadline: return "deadline";
            case RetryClass::RolledBack: return "rolled_back";
        }
        return "invalid";
    }

    namespace detail {
        inline bool icontains(std::string_view hay, std::string_view needle) noexcept {
            if (needle.empty() || hay.size() < needle.size()) return needle.empty();
            for (size_t i = 0; i + needle.size() <= hay.size(); ++i) {
                size_t k = 0;
                while (k < needle.size()) {
                    char a = hay[i + k], b = needle[k];
                    if (a >= 'A' && a <= 'Z') a = static_cast<char>(a - 'A' + 'a');
                    if (b >= 'A' && b <= 'Z') b = static_cast<char>(b - 'A' + 'a');
                    if (a != b) break;
                    ++k;
                }
                if (k == needle.size()) return true;
            }
            return false;
        }

        // Messages libpq produces itself when the connection is gone. They
        // carry no SQLSTATE, so the text is the only signal (pgbouncer's
        // "server conn crashed?" arrives the same way).
        inline bool is_connection_lost_message(std::string_view msg) noexcept {
            static constexpr std::string_view markers[] = {
                "server closed the connection",
                "ssl connection has been closed",
                "connection not open",
                "no connection to the server",
                "could not send data",
                "could not receive data",
                "ssl syscall error",
                "eof detected",
                "terminating connection",
                "connection reset",
                "broken pipe",
                "server conn crashed",
            };
            for (auto m : markers)
                if (icontains(msg, m)) return true;
            return false;
        }
    } // namespace detail

    // SQLSTATE part of the classification (empty sqlstate -> None).
    inline RetryClass classify_sqlstate_retry(std::string_view s) noexcept {
        if (s.size() != 5) return RetryClass::None;
        if (s == "08P01" || s == "28P01" || s == "53300" || s == "57P03") return RetryClass::NotStarted;
        if (s == "40001" || s == "40P01" || s == "55P03") return RetryClass::RolledBack;
        if (s == "57P01" || s == "57P02" || s == "25P03") return RetryClass::ConnectionLost;
        if (s[0] == '0' && s[1] == '8') return RetryClass::ConnectionLost;
        return RetryClass::None;
    }

    // Full classification of one failed operation. `io_timed_out` comes from
    // the connection that ran it (PgConnectionLibpq::io_timed_out()).
    inline RetryClass classify_retry(PgErrorCode code, const PgErrorDetail &d, std::string_view error,
                                     bool io_timed_out) noexcept {
        if (code == PgErrorCode::OK) return RetryClass::None;
        if (io_timed_out) return RetryClass::Deadline;
        if (const RetryClass by_state = classify_sqlstate_retry(d.sqlstate); by_state != RetryClass::None)
            return by_state;
        switch (code) {
            case PgErrorCode::ConnectionClosed:
            case PgErrorCode::SocketReadFailed:
                return error.starts_with("io deadline exceeded") ? RetryClass::Deadline
                                                                 : RetryClass::ConnectionLost;
            case PgErrorCode::ServerError:
                if (d.sqlstate.empty() &&
                    (detail::is_connection_lost_message(error) || detail::is_connection_lost_message(d.message)))
                    return RetryClass::ConnectionLost;
                return RetryClass::None;
            default:
                // AwaitCanceled must never be retried (cooperative cancellation),
                // InvalidFuture is transaction misuse, parser errors are bugs.
                return RetryClass::None;
        }
    }

    inline RetryClass classify_retry(const QueryResult &r, bool io_timed_out = false) noexcept {
        return r.ok ? RetryClass::None : classify_retry(r.code, r.err_detail, r.error, io_timed_out);
    }

    inline RetryClass classify_retry(const PgOpError &e) noexcept {
        return classify_retry(e.code, e.err_detail, e.error, false);
    }

    // One retry about to happen, handed to PgPool's retry observer before the
    // backoff sleep. Views point into the failed result and die with the call.
    struct RetryEvent {
        RetryClass cls{RetryClass::None};
        int attempt{0};             // 1-based number of the retry being made
        unsigned delay_ms{0};       // backoff before it
        PgErrorCode code{PgErrorCode::Unknown};
        std::string_view sqlstate;
        std::string_view message;
        std::string_view db;
        std::string_view user;
        bool in_transaction{false}; // run_tx() re-running a whole transaction
    };

    using RetryObserver = std::function<void(const RetryEvent &)>;
} // namespace usub::pg

#endif // PGRETRY_H
