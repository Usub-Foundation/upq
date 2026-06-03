// PgRouting.h
#ifndef PG_ROUTING_H
#define PG_ROUTING_H

#include <string>
#include <vector>
#include <memory>
#include <chrono>
#include <optional>
#include <cstdint>
#include <mutex>
#include "PgTransaction.h"
#include "upq/PgPool.h"

namespace usub::pg
{
    enum class NodeRole : uint8_t { Primary, SyncReplica, AsyncReplica, Analytics, Archive, Maintenance };

    enum class Consistency : uint8_t { Strong, BoundedStaleness, Eventual };

    enum class QueryKind : uint8_t { Read, Write, DDL, LongRead };

    struct BoundedStalenessCfg
    {
        std::chrono::milliseconds max_staleness{0};
        uint64_t max_lsn_lag{0};
    };

    struct RouteHint
    {
        QueryKind kind{QueryKind::Read};
        Consistency consistency{Consistency::Eventual};
        BoundedStalenessCfg staleness{};
        bool read_my_writes{false};
    };

    struct PgEndpoint
    {
        std::string name;
        std::string host, port, user, db, password;
        size_t max_pool{32};
        NodeRole role{NodeRole::AsyncReplica};
        uint8_t weight{1};
    };

    struct NodeStats
    {
        bool healthy{false};
        bool is_primary{false};
        std::chrono::milliseconds rtt{0};
        std::chrono::milliseconds replay_lag{0};
        uint64_t lsn_lag{0};
        uint32_t open_conns{0};
        uint32_t busy_conns{0};
    };

    struct PoolLimits
    {
        uint32_t default_max_conns{64};
        uint32_t analytics_max_conns{16};
    };

    struct TimeoutsMs
    {
        uint32_t connect{1500};
        uint32_t query_read{3000};
        uint32_t query_write{2000};
    };

    struct HealthCfg
    {
        uint32_t interval_ms{500};
        uint32_t lag_threshold_ms{120};
        std::string rtt_probe_sql{"SELECT 1"};
        uint32_t cb_quiet_ms{500};
        uint32_t cb_backoff_ms{1000};
        uint32_t cb_max_ms{1500};
        bool auto_detect_leader{true};
        std::string leader_probe_sql{"SELECT NOT pg_is_in_recovery()"};
    };

    struct RoutingCfg
    {
        Consistency default_consistency{Consistency::Eventual};
        BoundedStalenessCfg bounded_staleness{std::chrono::milliseconds{150}, 0};
        uint32_t read_my_writes_ttl_ms{500};
    };

    struct Config
    {
        std::vector<PgEndpoint> nodes;
        std::vector<std::string> primary_failover;
        RoutingCfg routing{};
        PoolLimits limits{};
        TimeoutsMs timeouts{};
        HealthCfg health{};
        uint32_t connect_retries{20};
        SSLConfig ssl_config{};
        TCPKeepaliveConfig keepalive_config{};
    };

    struct Row
    {
        int64_t lag_ms;
        int64_t lsn_lag;
    };

    class PgConnector
    {
    public:
        explicit PgConnector(Config cfg);

        PgPool* route(const RouteHint& hint);

        PgPool* route_for_tx(const PgTransactionConfig& tx);

        usub::uvent::task::Awaitable<void> start_health_loop();

        usub::uvent::task::Awaitable<void> health_tick();

        PgPool* pin(const std::string& node_name, const RouteHint&);

        const Config& config() const { return this->cfg_; }

    private:
        struct Node
        {
            PgEndpoint ep;
            std::unique_ptr<PgPool> pool;
            NodeStats stats;
            uint8_t cb_state{0};
            std::chrono::steady_clock::time_point cb_until{};
        };

        Config cfg_;
        std::vector<Node> nodes_;
        std::vector<size_t> primary_failover_idx_;

        Node* pick_primary();
        Node* pick_best_replica(const RouteHint& hint);
        Node* pick_any(bool prefer_primary);
        bool ensure_pool(Node& n);
        static bool is_replica(NodeRole r);
        static bool is_usable(NodeRole r);
        usub::uvent::task::Awaitable<bool> probe_healthy(PgPool& pool);
        usub::uvent::task::Awaitable<bool> probe_is_primary(PgPool& pool);
        usub::uvent::task::Awaitable<std::chrono::milliseconds> probe_rtt(PgPool& pool, const std::string& sql);
        usub::uvent::task::Awaitable<std::pair<std::chrono::milliseconds, uint64_t>>
        probe_replication_lag(PgPool& pool);
        void apply_circuit_breaker(Node& n, bool ok);
    };
}
#endif