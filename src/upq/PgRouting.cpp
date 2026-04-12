// PgRouting.cpp
#include "upq/PgRouting.h"
#include "uvent/Uvent.h"
#include <algorithm>
#include <chrono>
#include <cstdio>

using namespace std::chrono;

namespace usub::pg {
    static size_t pool_cap_for(NodeRole role, const PoolLimits &lim) {
        return (role == NodeRole::Analytics) ? lim.analytics_max_conns : lim.default_max_conns;
    }

    bool PgConnector::is_replica(NodeRole r) {
        return r == NodeRole::SyncReplica || r == NodeRole::AsyncReplica || r == NodeRole::Analytics;
    }

    bool PgConnector::is_usable(NodeRole r) {
        return r != NodeRole::Archive && r != NodeRole::Maintenance;
    }

    PgConnector::PgConnector(Config cfg) : cfg_(std::move(cfg)) {
        this->nodes_.reserve(this->cfg_.nodes.size());
        for (const auto &ep: this->cfg_.nodes) {
            std::unique_ptr<PgPool> pool;
            try {
                pool = std::make_unique<PgPool>(
                    ep.host, ep.port, ep.user, ep.db, ep.password,
                    ep.max_pool ? ep.max_pool : pool_cap_for(ep.role, this->cfg_.limits),
                    this->cfg_.connect_retries, this->cfg_.ssl_config, this->cfg_.keepalive_config
                );
            } catch (const std::exception &e) {
                std::fprintf(stderr,
                    "[UPQ/routing] WARNING: failed to construct pool for node '%s': %s\n",
                    ep.name.c_str(), e.what());
            } catch (...) {
                std::fprintf(stderr,
                    "[UPQ/routing] WARNING: failed to construct pool for node '%s': "
                    "unknown exception\n",
                    ep.name.c_str());
            }
            this->nodes_.push_back(Node{ep, std::move(pool), {}});
        }
        for (const auto &name: this->cfg_.primary_failover) {
            auto it = std::find_if(this->nodes_.begin(), this->nodes_.end(),
                                   [&](const Node &n) { return n.ep.name == name; });
            if (it != this->nodes_.end())
                this->primary_failover_idx_.push_back(size_t(std::distance(this->nodes_.begin(), it)));
        }
        if (this->primary_failover_idx_.empty()) {
            for (size_t i = 0; i < this->nodes_.size(); ++i)
                if (this->nodes_[i].ep.role == NodeRole::Primary)
                    this->primary_failover_idx_.push_back(i);
            for (size_t i = 0; i < this->nodes_.size(); ++i)
                if (this->nodes_[i].ep.role == NodeRole::SyncReplica)
                    this->primary_failover_idx_.push_back(i);
            for (size_t i = 0; i < this->nodes_.size(); ++i)
                if (this->nodes_[i].ep.role == NodeRole::AsyncReplica)
                    this->primary_failover_idx_.push_back(i);
        }
    }

    bool PgConnector::ensure_pool(Node &n) {
        if (n.pool) return true;
        try {
            const size_t cap = n.ep.max_pool ? n.ep.max_pool : pool_cap_for(n.ep.role, this->cfg_.limits);
            n.pool = std::make_unique<PgPool>(n.ep.host, n.ep.port, n.ep.user, n.ep.db, n.ep.password, cap,
                                              this->cfg_.connect_retries, this->cfg_.ssl_config,
                                              this->cfg_.keepalive_config);
            return true;
        } catch (const std::exception &e) {
            std::fprintf(stderr,
                "[UPQ/routing] WARNING: ensure_pool failed for node '%s': %s\n",
                n.ep.name.c_str(), e.what());
            n.pool.reset();
            return false;
        } catch (...) {
            std::fprintf(stderr,
                "[UPQ/routing] WARNING: ensure_pool failed for node '%s': unknown exception\n",
                n.ep.name.c_str());
            n.pool.reset();
            return false;
        }
    }

    PgPool *PgConnector::route(const RouteHint &hint) {
        if (hint.kind == QueryKind::Write ||
            hint.kind == QueryKind::DDL ||
            hint.consistency == Consistency::Strong ||
            hint.read_my_writes) {
            if (auto *p = this->pick_primary())
                if (this->ensure_pool(*p))
                    return p->pool.get();

            if (auto *any = this->pick_any(true))
                return any->pool.get();

            return nullptr;
        }

        if (auto *r = this->pick_best_replica(hint))
            if (this->ensure_pool(*r))
                return r->pool.get();

        if (auto *p = this->pick_primary())
            if (this->ensure_pool(*p))
                return p->pool.get();

        if (auto *any = this->pick_any(false))
            return any->pool.get();

        return nullptr;
    }


    PgPool *PgConnector::route_for_tx(const PgTransactionConfig &cfg_tx) {
        const auto eff_consistency =
                (cfg_tx.isolation == TxIsolationLevel::Serializable)
                    ? Consistency::Strong
                    : this->cfg_.routing.default_consistency;

        if (!cfg_tx.read_only || eff_consistency == Consistency::Strong) {
            if (auto *p = this->pick_primary())
                if (this->ensure_pool(*p))
                    return p->pool.get();

            if (auto *any = this->pick_any(true))
                return any->pool.get();

            return nullptr;
        }

        if (cfg_tx.deferrable) {
            Node *best = nullptr;
            for (auto &n: this->nodes_) {
                if (n.ep.role != NodeRole::SyncReplica ||
                    !this->is_usable(n.ep.role) ||
                    n.cb_state == 2)
                    continue;
                if (!n.pool || !n.stats.healthy)
                    continue;
                if (!best || n.stats.replay_lag < best->stats.replay_lag)
                    best = &n;
            }
            if (best && this->ensure_pool(*best))
                return best->pool.get();

            if (auto *p = this->pick_primary())
                if (this->ensure_pool(*p))
                    return p->pool.get();

            if (auto *any = this->pick_any(true))
                return any->pool.get();

            return nullptr;
        }

        RouteHint rh{
            .kind = QueryKind::Read,
            .consistency = eff_consistency,
            .staleness = this->cfg_.routing.bounded_staleness,
            .read_my_writes = false
        };

        if (auto *r = this->pick_best_replica(rh))
            if (this->ensure_pool(*r))
                return r->pool.get();

        if (auto *p = this->pick_primary())
            if (this->ensure_pool(*p))
                return p->pool.get();

        if (auto *any = this->pick_any(false))
            return any->pool.get();

        return nullptr;
    }


    PgConnector::Node *PgConnector::pick_primary() {
        for (auto idx: this->primary_failover_idx_) {
            auto &n = this->nodes_[idx];
            if (n.ep.role == NodeRole::Primary && this->is_usable(n.ep.role) && n.cb_state != 2 && n.stats.healthy && n.
                pool)
                return &n;
        }
        for (auto &n: this->nodes_)
            if (n.ep.role == NodeRole::Primary && this->is_usable(n.ep.role) && n.cb_state != 2 && n.pool)
                return &n;
        return nullptr;
    }

    PgConnector::Node *PgConnector::pick_best_replica(const RouteHint &hint) {
        Node *best = nullptr;
        auto ok_stale = [&](const Node &n)-> bool {
            if (hint.consistency != Consistency::BoundedStaleness) return true;
            if (n.stats.replay_lag > hint.staleness.max_staleness) return false;
            if (hint.staleness.max_lsn_lag && n.stats.lsn_lag > hint.staleness.max_lsn_lag) return false;
            return true;
        };
        auto better = [&](const Node *a, const Node *b)-> bool {
            if (a->stats.rtt != b->stats.rtt) return a->stats.rtt < b->stats.rtt;
            return a->ep.weight > b->ep.weight;
        };
        for (auto &n: this->nodes_) {
            if (!this->is_replica(n.ep.role) || !this->is_usable(n.ep.role) || n.cb_state == 2) continue;
            if (!n.pool || !n.stats.healthy) continue;
            if (!ok_stale(n)) continue;
            if (!best || better(&n, best)) best = &n;
        }
        return best;
    }

    PgConnector::Node *PgConnector::pick_any(bool prefer_primary) {
        Node *primary = nullptr;
        Node *any_replica = nullptr;

        for (auto &n: this->nodes_) {
            if (!this->is_usable(n.ep.role))
                continue;
            if (!n.pool && !this->ensure_pool(n))
                continue;

            if (n.ep.role == NodeRole::Primary) {
                if (!primary) primary = &n;
            } else if (this->is_replica(n.ep.role)) {
                if (!any_replica) any_replica = &n;
            }
        }

        if (prefer_primary)
            return primary ? primary : any_replica;
        return any_replica ? any_replica : primary;
    }

    void PgConnector::apply_circuit_breaker(Node &n, bool ok) {
        using clock = std::chrono::steady_clock;
        auto now = clock::now();
        const auto quiet = std::chrono::milliseconds{this->cfg_.health.cb_quiet_ms};
        const auto backoff = std::chrono::milliseconds{this->cfg_.health.cb_backoff_ms};
        const auto maxb = std::chrono::milliseconds{this->cfg_.health.cb_max_ms};
        if (ok) {
            if (n.cb_state == 1 && now >= n.cb_until) n.cb_state = 0;
            if (n.cb_state == 2 && now >= n.cb_until) n.cb_state = 1;
            return;
        }
        if (n.cb_state == 0) {
            n.cb_state = 2;
            n.cb_until = now + quiet;
        } else if (n.cb_state == 1) {
            n.cb_state = 2;
            n.cb_until = now + backoff;
        } else n.cb_until = now + maxb;
    }

    usub::uvent::task::Awaitable<bool> PgConnector::probe_healthy(PgPool &pool) {
        auto qr = co_await pool.query_awaitable("SELECT 1");
        co_return qr.ok;
    }

    usub::uvent::task::Awaitable<std::chrono::milliseconds>
    PgConnector::probe_rtt(PgPool &pool, const std::string &sql) {
        using clock = std::chrono::steady_clock;
        auto t0 = clock::now();
        auto qr = co_await pool.query_awaitable(sql);
        auto t1 = clock::now();
        if (!qr.ok) co_return milliseconds{9999};
        co_return std::chrono::duration_cast<milliseconds>(t1 - t0);
    }

    usub::uvent::task::Awaitable<std::pair<std::chrono::milliseconds, uint64_t> >
    PgConnector::probe_replication_lag(PgPool &pool) {
        auto opt = co_await pool.query_reflect_one<Row>(R"SQL(
        SELECT
          COALESCE( (EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())) * 1000)::bigint, 0 ) AS lag_ms,
          COALESCE( pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint, 0 ) AS lsn_lag
    )SQL");
        if (!opt.has_value()) co_return std::make_pair(milliseconds{0}, uint64_t{0});
        co_return std::make_pair(milliseconds{opt->lag_ms}, static_cast<uint64_t>(opt->lsn_lag));
    }

    usub::uvent::task::Awaitable<void> PgConnector::health_tick() {
        const auto lag_thr = std::chrono::milliseconds{this->cfg_.health.lag_threshold_ms};
        for (auto &n: this->nodes_) {
            if (!this->is_usable(n.ep.role)) continue;
            if (!this->ensure_pool(n)) {
                n.stats.healthy = false;
                this->apply_circuit_breaker(n, false);
                continue;
            }
            bool ok = co_await this->probe_healthy(*n.pool);
            auto rtt = co_await this->probe_rtt(*n.pool, this->cfg_.health.rtt_probe_sql);
            auto [lag_ms, lsn_lag] = co_await this->probe_replication_lag(*n.pool);
            n.stats.healthy = ok;
            n.stats.rtt = rtt;
            n.stats.replay_lag = lag_ms;
            n.stats.lsn_lag = lsn_lag;
            if (n.stats.replay_lag > lag_thr) n.stats.healthy = false;
            if (n.ep.role == NodeRole::Primary && n.stats.replay_lag.count() > 0) n.stats.healthy = false;
            this->apply_circuit_breaker(n, n.stats.healthy);
        }
        co_return;
    }

    usub::uvent::task::Awaitable<void> PgConnector::start_health_loop() {
        auto iv = std::chrono::milliseconds{this->cfg_.health.interval_ms ? this->cfg_.health.interval_ms : 500};
        while (true) {
            co_await this->health_tick();
            co_await usub::uvent::system::this_coroutine::sleep_for(iv);
        }
    }

    PgPool *PgConnector::pin(const std::string &node_name, const RouteHint &) {
        auto it = std::find_if(this->nodes_.begin(), this->nodes_.end(),
                               [&](const Node &n) { return n.ep.name == node_name; });
        if (it == this->nodes_.end() || !this->is_usable(it->ep.role) || it->cb_state == 2) return nullptr;
        if (!this->ensure_pool(*it) || !it->stats.healthy) return nullptr;
        return it->pool.get();
    }
}
