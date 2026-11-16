#pragma once
#include "database.hpp"
namespace spark {
class ActionStore {
    DatabaseConfig config_;
public:
    explicit ActionStore(DatabaseConfig config):config_(std::move(config)){}
    void verify() const;
    nlohmann::json projects(const std::string& name,const std::string& id) const;
    nlohmann::json project_files(const std::string& id,const nlohmann::json& after = nullptr) const;
    nlohmann::json project_boards(const std::string& id) const;
    std::optional<nlohmann::json> get(const std::string& id,const std::string& owner) const;
    nlohmann::json create_timer(const std::string& id,const std::string& owner,const std::string& fingerprint,const nlohmann::json& packet) const;
    nlohmann::json undo(const std::string& id,const std::string& owner) const;
    nlohmann::json timers() const;
    nlohmann::json create_event(const std::string&,const std::string&,const std::string&,const nlohmann::json&) const;
    std::optional<nlohmann::json> event(const std::string&) const;
    nlohmann::json find_events(const nlohmann::json&) const;
    nlohmann::json propose_event(const std::string& id,const std::string& owner,const std::string& fingerprint,const std::string& conversation,const std::string& event_id,const std::string& kind,const nlohmann::json& changes,const std::string& scope,const nlohmann::json& source,const nlohmann::json& expected=nullptr) const;
    std::optional<nlohmann::json> confirmation(const std::string& id,const std::string& owner) const;
    std::optional<nlohmann::json> latest_confirmation(const std::string& owner,const std::string& conversation) const;
    std::optional<nlohmann::json> decision(const std::string& request,const std::string& owner,const std::string& fingerprint) const;
    nlohmann::json decide(const std::string& id,const std::string& owner,bool confirm,const std::string& request="",const std::string& fingerprint="") const;
    void supersede(const std::string& owner,const std::string& conversation) const;
    nlohmann::json occurrences(const nlohmann::json&) const;
};
}
