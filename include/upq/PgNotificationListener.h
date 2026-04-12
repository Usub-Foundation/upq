#ifndef PGNOTIFICATIONLISTENER_H
#define PGNOTIFICATIONLISTENER_H

#include <string>
#include <string_view>
#include <utility>
#include <memory>
#include <concepts>
#include <iostream>

#include <libpq-fe.h>

#include "PgConnection.h"
#include "PgTypes.h"
#include "uvent/Uvent.h"

namespace usub::pg
{
    namespace detail_listener {
        inline std::optional<std::string> quote_ident_strict(const std::string& ident) {
            if (ident.empty()) return std::nullopt;
            for (char c : ident) {
                if (c == '\0') return std::nullopt;
            }
            std::string out;
            out.reserve(ident.size() + 2);
            out.push_back('"');
            for (char c : ident) {
                if (c == '"') out.push_back('"');
                out.push_back(c);
            }
            out.push_back('"');
            return out;
        }
    }

    template <class HandlerT>
        requires PgNotifyHandler<HandlerT>
    class PgNotificationListener
    {
    public:
        PgNotificationListener(std::string channel,
                               std::shared_ptr<PgConnectionLibpq> conn)
            : channel_(std::move(channel))
              , conn_(std::move(conn))
        {
        }

        void setHandler(HandlerT h)
        {
            this->handler_ = std::move(h);
            this->has_handler_ = true;
        }

        usub::uvent::task::Awaitable<void> run()
        {
            if (!this->conn_ || !this->conn_->connected())
            {
                co_return;
            }

            {
                auto q = detail_listener::quote_ident_strict(this->channel_);
                if (!q) {
                    co_return;
                }
                std::string listen_sql = "LISTEN " + *q + ";";
                QueryResult qr = co_await this->conn_->exec_simple_query_nonblocking(listen_sql);
                if (!qr.ok)
                {
                    co_return;
                }
            }

            while (true)
            {
                co_await this->conn_->wait_readable_for_listener();

                PGconn* raw = this->conn_->raw_conn();
                if (!raw)
                {
                    co_return;
                }

                if (PQconsumeInput(raw) == 0)
                {
                    if (PQstatus(raw) == CONNECTION_BAD)
                    {
                        co_return;
                    }
                    co_await usub::uvent::system::this_coroutine::sleep_for(
                        std::chrono::milliseconds(10));
                    continue;
                }

                while (true)
                {
                    PGnotify* n = PQnotifies(raw);
                    if (!n) break;

                    const char* ch_raw = n->relname ? n->relname : "";
                    const char* pl_raw = n->extra ? n->extra : "";
                    int be_pid = n->be_pid;

                    if (this->has_handler_)
                    {
                        dispatch_async(ch_raw, pl_raw, be_pid);
                    }

                    PQfreemem(n);
                }
            }

            co_return;
        }

    private:
        usub::uvent::task::Awaitable<void>
        run_single_handler(
            HandlerT handler_copy,
            std::string ch_copy,
            std::string payload_copy,
            int pid_copy
        )
        {
            co_await handler_copy(
                std::move(ch_copy),
                std::move(payload_copy),
                pid_copy
            );
            co_return;
        }

        void dispatch_async(std::string_view ch,
                            std::string_view payload,
                            int pid)
        {
            std::string ch_copy(ch);
            std::string payload_copy(payload);
            int pid_copy = pid;

            HandlerT handler_copy = this->handler_;

            usub::uvent::system::co_spawn(
                this->run_single_handler(
                    std::move(handler_copy),
                    std::move(ch_copy),
                    std::move(payload_copy),
                    pid_copy
                )
            );
        }

    private:
        std::string channel_;
        std::shared_ptr<PgConnectionLibpq> conn_;

        HandlerT handler_{};
        bool has_handler_{false};
    };
}

#endif // PGNOTIFICATIONLISTENER_H
