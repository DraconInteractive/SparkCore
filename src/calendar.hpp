#pragma once
#include "authorization.hpp"
#include "database.hpp"
#include <httplib.h>
#include <memory>
#include <ctime>
#include <mutex>
#include <condition_variable>
#include <thread>
namespace spark {
class CalendarStore {
public:
    explicit CalendarStore(DatabaseConfig config) : config_(std::move(config)) {}
    void verify() const;
    void tick() const;
    nlohmann::json sync_info() const;
    nlohmann::json sync_changes(uint64_t after, uint64_t through = 0) const;
    nlohmann::json sync_item(const std::string& id) const;
    nlohmann::json sync_apply(const nlohmann::json& packet) const;
    nlohmann::json sync_state(const std::string& connection) const;
    nlohmann::json sync_baseline(const std::string& connection,const std::string& id) const;
    nlohmann::json sync_conflicts(const std::string& connection,uint64_t after) const;
    void sync_finish(const std::string& connection,const std::string& id,const nlohmann::json& baseline,const nlohmann::json& state,const nlohmann::json& conflict=nullptr) const;
    void sync_save(const std::string& connection, const nlohmann::json& state) const;

    nlohmann::json create(const nlohmann::json& packet) const;
    std::optional<nlohmann::json> get(const std::string& id, const std::optional<std::string>& calendar = std::nullopt) const;
    std::optional<nlohmann::json> patch(const std::string& id, const nlohmann::json& packet, const std::optional<std::string>& calendar = std::nullopt) const;
    std::optional<nlohmann::json> move(const std::string& id, const nlohmann::json& packet) const;
    bool remove(const std::string& id, const std::optional<std::string>& calendar = std::nullopt) const;
    nlohmann::json list(const nlohmann::json& filters, bool expand = false) const;
private:
    DatabaseConfig config_;
};
// Validate and normalize the six portable event fields; throws invalid_argument.
nlohmann::json calendar_sync_data(nlohmann::json data);
// First occurrence at or after the supplied UTC instant. Weekly rules use Perth time.
std::optional<std::time_t> calendar_occurrence(const nlohmann::json& event, std::time_t from);
std::time_t calendar_instant(const std::string& timestamp);
std::string calendar_timestamp(std::time_t instant);
class CalendarScheduler {
public:
    explicit CalendarScheduler(std::shared_ptr<CalendarStore> store);
    ~CalendarScheduler();
    CalendarScheduler(const CalendarScheduler&) = delete;
    CalendarScheduler& operator=(const CalendarScheduler&) = delete;
private:
    std::mutex mutex_;
    std::condition_variable wake_;
    bool stopping_ = false;
    std::thread worker_;
};
void configure_calendar(httplib::Server& server, std::shared_ptr<CalendarStore> store, std::shared_ptr<Authorization> auth = nullptr);
nlohmann::json calendar_endpoints();
}
