#include "upq/PgTransaction.h"
#include <atomic>
#include <cstdio>

namespace usub::pg
{
    std::string PgTransaction::build_begin_sql(const PgTransactionConfig& cfg)
    {
        bool any_opts =
            (cfg.isolation != TxIsolationLevel::Default) ||
            cfg.read_only ||
            cfg.deferrable;

        if (!any_opts) return "BEGIN";

        std::string out = "BEGIN";

        switch (cfg.isolation)
        {
        case TxIsolationLevel::ReadCommitted:
            out += " ISOLATION LEVEL READ COMMITTED";
            break;
        case TxIsolationLevel::RepeatableRead:
            out += " ISOLATION LEVEL REPEATABLE READ";
            break;
        case TxIsolationLevel::Serializable:
            out += " ISOLATION LEVEL SERIALIZABLE";
            break;
        case TxIsolationLevel::Default:
        default:
            break;
        }

        out += (cfg.read_only ? " READ ONLY" : " READ WRITE");
        if (cfg.deferrable) out += " DEFERRABLE";
        return out;
    }

    PgTransaction::PgTransaction(PgPool* pool, PgTransactionConfig cfg)
        : pool_(pool)
          , cfg_(cfg)
    {
        if (cfg_.read_only && !cfg_.deferrable)
        {
            emulate_readonly_autocommit_ = true;
        }
    }

    PgTransaction::~PgTransaction()
    {
        if (this->conn_)
        {
            if (this->active_ && !this->committed_ && !this->rolled_back_)
            {
                std::fprintf(stderr,
                    "[UPQ/tx] WARNING: PgTransaction destroyed while active; "
                    "connection will be closed to force implicit rollback. "
                    "Always call commit()/rollback()/finish() explicitly.\n");
            }
            this->pool_->mark_dead(this->conn_);
            this->conn_.reset();
        }
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::begin()
    {
        auto err = co_await begin_errored();
        co_return !err.has_value();
    }

    usub::uvent::task::Awaitable<std::optional<PgOpError>>
    PgTransaction::begin_errored()
    {
        if (active_)
        {
            co_return std::nullopt;
        }

        auto c = co_await pool_->acquire_connection();
        if (!c)
        {
            conn_.reset();
            PgOpError err{
                PgErrorCode::ConnectionClosed,
                "failed to acquire connection from pool",
                {}
            };
            co_return std::make_optional(std::move(err));
        }

        conn_ = *c;
        if (!conn_ || !conn_->connected())
        {
            PgOpError err{
                PgErrorCode::ConnectionClosed,
                "connection not OK",
                {}
            };
            if (conn_)
                pool_->mark_dead(conn_);
            conn_.reset();
            co_return std::make_optional(std::move(err));
        }

        if (emulate_readonly_autocommit_)
        {
            active_ = true;
            committed_ = false;
            rolled_back_ = false;
            co_return std::nullopt;
        }

        const std::string bsql = build_begin_sql(cfg_);
        QueryResult r_begin = co_await pool_->query_on(conn_, bsql);
        if (!r_begin.ok)
        {
            PgOpError err{r_begin.code, r_begin.error, r_begin.err_detail};
            pool_->mark_dead(conn_);
            conn_.reset();

            active_ = false;
            committed_ = false;
            rolled_back_ = false;
            co_return std::make_optional(std::move(err));
        }

        active_ = true;
        committed_ = false;
        rolled_back_ = false;
        co_return std::nullopt;
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::commit()
    {
        if (!active_) co_return false;

        if (!conn_ || !conn_->connected())
        {
            committed_ = false;
            rolled_back_ = true;
            active_ = false;
            if (conn_)
            {
                pool_->mark_dead(conn_);
                conn_.reset();
            }
            co_return false;
        }

        if (emulate_readonly_autocommit_)
        {
            committed_ = true;
            rolled_back_ = false;
            active_ = false;
            co_await pool_->release_connection_async(conn_);
            conn_.reset();
            co_return true;
        }

        QueryResult r_commit = co_await pool_->query_on(conn_, "COMMIT");
        if (!r_commit.ok)
        {

            if (is_fatal_connection_error(r_commit))
            {
                pool_->mark_dead(conn_);
            }
            else
            {
                co_await pool_->release_connection_async(conn_);
            }

            committed_ = false;
            rolled_back_ = true;
            active_ = false;
            conn_.reset();
            co_return false;
        }

        committed_ = true;
        rolled_back_ = false;
        active_ = false;
        co_await pool_->release_connection_async(conn_);
        conn_.reset();
        co_return true;
    }

    usub::uvent::task::Awaitable<void> PgTransaction::rollback()
    {
        if (!active_) co_return;

        if (emulate_readonly_autocommit_)
        {
            committed_ = false;
            rolled_back_ = true;
            active_ = false;
            if (conn_)
            {
                co_await pool_->release_connection_async(conn_);
                conn_.reset();
            }
            co_return;
        }

        if (conn_ && conn_->connected())
        {
            QueryResult r_rb = co_await pool_->query_on(conn_, "ROLLBACK");
            if (is_fatal_connection_error(r_rb))
            {
                pool_->mark_dead(conn_);
                conn_.reset();
            }
        }

        committed_ = false;
        rolled_back_ = true;
        active_ = false;
        if (conn_)
        {
            co_await pool_->release_connection_async(conn_);
            conn_.reset();
        }
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgTransaction::finish()
    {
        if (!active_)
        {
            if (conn_)
            {
                co_await pool_->release_connection_async(conn_);
                conn_.reset();
            }
            co_return;
        }
        co_await rollback();
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgTransaction::abort()
    {
        if (!active_) co_return;

        if (emulate_readonly_autocommit_)
        {
            committed_ = false;
            rolled_back_ = true;
            active_ = false;
            if (conn_)
            {
                co_await pool_->release_connection_async(conn_);
                conn_.reset();
            }
            co_return;
        }

        if (conn_ && conn_->connected())
        {
            QueryResult r_rb = co_await pool_->query_on(conn_, "ABORT");
            if (is_fatal_connection_error(r_rb))
            {
                pool_->mark_dead(conn_);
                conn_.reset();
            }
        }

        committed_ = false;
        rolled_back_ = true;
        active_ = false;
        if (conn_)
        {
            co_await pool_->release_connection_async(conn_);
            conn_.reset();
        }
        co_return;
    }

    usub::uvent::task::Awaitable<bool> PgTransaction::send_sql_nocheck(const std::string& sql)
    {
        if (!active_ || !conn_ || !conn_->connected()) co_return false;
        QueryResult r = co_await pool_->query_on(conn_, sql);
        if (is_fatal_connection_error(r))
        {
            pool_->mark_dead(conn_);
            conn_.reset();
            active_ = false;
            rolled_back_ = true;
            committed_ = false;
            co_return false;
        }
        co_return r.ok;
    }

    static std::atomic<uint64_t> g_subtx_id{0};

    PgTransaction::PgSubtransaction PgTransaction::make_subtx()
    {
        uint64_t id = g_subtx_id.fetch_add(1, std::memory_order_relaxed);
        std::string name = "uv_sp_" + std::to_string(id);
        return PgSubtransaction(*this, std::move(name));
    }

    PgTransaction::PgSubtransaction::PgSubtransaction(
        PgTransaction& parent,
        std::string savepoint_name)
        : parent_(parent), sp_name_(std::move(savepoint_name))
    {
    }

    PgTransaction::PgSubtransaction::~PgSubtransaction() = default;

    usub::uvent::task::Awaitable<bool>
    PgTransaction::PgSubtransaction::begin()
    {
        if (!parent_.active_ || !parent_.conn_ || !parent_.conn_->connected()) co_return false;
        if (parent_.emulate_readonly_autocommit_) co_return false;

        std::string cmd = "SAVEPOINT " + sp_name_;
        QueryResult r = co_await parent_.pool_->query_on(parent_.conn_, cmd);
        if (!r.ok)
        {
            if (is_fatal_connection_error(r))
            {
                parent_.pool_->mark_dead(parent_.conn_);
                parent_.conn_.reset();
                parent_.active_ = false;
                parent_.rolled_back_ = true;
                parent_.committed_ = false;
            }
            co_return false;
        }

        active_ = true;
        committed_ = false;
        rolled_back_ = false;
        co_return true;
    }

    usub::uvent::task::Awaitable<bool>
    PgTransaction::PgSubtransaction::commit()
    {
        if (!active_) co_return false;
        if (!parent_.conn_ || !parent_.conn_->connected())
        {
            active_ = false;
            committed_ = false;
            rolled_back_ = true;
            co_return false;
        }
        if (parent_.emulate_readonly_autocommit_) co_return false;

        std::string cmd = "RELEASE SAVEPOINT " + sp_name_;
        QueryResult r = co_await parent_.pool_->query_on(parent_.conn_, cmd);
        if (!r.ok)
        {
            if (is_fatal_connection_error(r))
            {
                parent_.pool_->mark_dead(parent_.conn_);
                parent_.conn_.reset();
                parent_.active_ = false;
                parent_.rolled_back_ = true;
                parent_.committed_ = false;
            }
            active_ = false;
            committed_ = false;
            rolled_back_ = true;
            co_return false;
        }

        active_ = false;
        committed_ = true;
        rolled_back_ = false;
        co_return true;
    }

    usub::uvent::task::Awaitable<void>
    PgTransaction::PgSubtransaction::rollback()
    {
        if (!active_) co_return;
        if (parent_.emulate_readonly_autocommit_)
        {
            active_ = false;
            committed_ = false;
            rolled_back_ = true;
            co_return;
        }

        if (parent_.conn_ && parent_.conn_->connected())
        {
            std::string cmd = "ROLLBACK TO SAVEPOINT " + sp_name_;
            QueryResult r = co_await parent_.pool_->query_on(parent_.conn_, cmd);
            if (is_fatal_connection_error(r))
            {
                parent_.pool_->mark_dead(parent_.conn_);
                parent_.conn_.reset();
                parent_.active_ = false;
                parent_.rolled_back_ = true;
                parent_.committed_ = false;
            }
        }

        active_ = false;
        committed_ = false;
        rolled_back_ = true;
        co_return;
    }
} // namespace usub::pg
