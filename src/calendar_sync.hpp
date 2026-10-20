#pragma once
#include "calendar.hpp"
#include <functional>
namespace spark {
using CalendarRemote = std::function<nlohmann::json(const std::string&,const std::string&,const nlohmann::json&)>;
void run_calendar_sync(const CalendarStore&,const std::string& connection,const CalendarRemote&);
nlohmann::json calendar_sync_status(const nlohmann::json&);
void configure_calendar_sync(httplib::Server&,std::shared_ptr<CalendarStore>,std::shared_ptr<Authorization>);
nlohmann::json calendar_sync_endpoints();
}
