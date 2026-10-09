#pragma once
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
    nlohmann::json create(const nlohmann::json& packet) const;
    std::optional<nlohmann::json> get(const std::string& id) const;
    std::optional<nlohmann::json> patch(const std::string& id, const nlohmann::json& packet) const;
    bool remove(const std::string& id) const;
    nlohmann::json list(const nlohmann::json& filters, bool expand = false) const;
private:
    DatabaseConfig config_;
};
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
void configure_calendar(httplib::Server& server, std::shared_ptr<CalendarStore> store);
nlohmann::json calendar_endpoints();
}
