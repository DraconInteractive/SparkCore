#pragma once
#include "authorization.hpp"
#include "database.hpp"
#include <httplib.h>
namespace spark {
struct ConfigConflict:std::runtime_error {
    uint64_t revision;
    explicit ConfigConflict(uint64_t value):std::runtime_error("revision_conflict"),revision(value){}
};
class ConfigStore {
    DatabaseConfig config_;
public:
    explicit ConfigStore(DatabaseConfig config):config_(std::move(config)){}
    void verify() const;
    nlohmann::json create(const nlohmann::json&) const;
    nlohmann::json list(const nlohmann::json&) const;
    std::optional<nlohmann::json> get(const std::string&) const;
    std::optional<nlohmann::json> update(const std::string&,const nlohmann::json&) const;
    bool remove(const std::string&,uint64_t) const;
};
void configure_configs(httplib::Server&,std::shared_ptr<ConfigStore>,std::shared_ptr<Authorization>);
nlohmann::json config_endpoints();
}
