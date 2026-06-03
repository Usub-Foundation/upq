//
// Created by root on 11/3/25.
//

#ifndef PGROUTINGBUILDER_H
#define PGROUTINGBUILDER_H

#include "PgRouting.h"
#include <unordered_set>

namespace usub::pg {
    class PgConnectorBuilder {
    public:
        PgConnectorBuilder &node(std::string name,
                                 std::string host, std::string port,
                                 std::string user, std::string db, std::string password,
                                 NodeRole role, uint8_t weight = 1, size_t max_pool = 32) {
            this->cfg_.nodes.push_back(PgEndpoint{
                std::move(name), std::move(host), std::move(port),
                std::move(user), std::move(db), std::move(password),
                max_pool, role, weight
            });
            return *this;
        }

        PgConnectorBuilder &primary_failover(std::initializer_list<std::string> order) {
            this->cfg_.primary_failover.assign(order.begin(), order.end());
            return *this;
        }

        PgConnectorBuilder &default_consistency(Consistency c) {
            this->cfg_.routing.default_consistency = c;
            return *this;
        }

        PgConnectorBuilder &bounded_staleness(std::chrono::milliseconds ms, uint64_t lsn = 0) {
            this->cfg_.routing.bounded_staleness = {ms, lsn};
            return *this;
        }

        PgConnectorBuilder &read_my_writes_ttl(std::chrono::milliseconds ttl) {
            this->cfg_.routing.read_my_writes_ttl_ms = (uint32_t) ttl.count();
            return *this;
        }

        PgConnectorBuilder &pool_limits(uint32_t def_max, uint32_t olap_max) {
            this->cfg_.limits = {def_max, olap_max};
            return *this;
        }

        PgConnectorBuilder &timeouts(uint32_t connect, uint32_t qread, uint32_t qwrite) {
            this->cfg_.timeouts = {connect, qread, qwrite};
            return *this;
        }

        PgConnectorBuilder &health(uint32_t interval_ms, uint32_t lag_thr_ms, std::string probe_sql = "SELECT 1") {
            this->cfg_.health.interval_ms = interval_ms;
            this->cfg_.health.lag_threshold_ms = lag_thr_ms;
            this->cfg_.health.rtt_probe_sql = std::move(probe_sql);
            return *this;
        }

        PgConnectorBuilder &auto_detect_leader(bool on) {
            this->cfg_.health.auto_detect_leader = on;
            return *this;
        }

        PgConnectorBuilder &leader_probe_sql(std::string sql) {
            this->cfg_.health.leader_probe_sql = std::move(sql);
            return *this;
        }

        template<typename CFG>
        PgConnectorBuilder &ssl_config(CFG &&ssl_config) requires std::same_as<std::remove_cvref_t<CFG>, SSLConfig> {
            this->cfg_.ssl_config = std::forward<CFG>(ssl_config);
            return *this;
        }

        template<typename CFG>
        PgConnectorBuilder &keepalive_config(CFG &&keepalive_config) requires std::same_as<std::remove_cvref_t<CFG>,
            TCPKeepaliveConfig> {
            this->cfg_.keepalive_config = std::forward<CFG>(keepalive_config);
            return *this;
        }

        PgConnector build() {
            validate();
            return usub::pg::PgConnector(std::move(this->cfg_));
        }

        const Config &config() const { return this->cfg_; }

    private:
        Config cfg_;

        void validate();
    };
}

#endif //PGROUTINGBUILDER_H
