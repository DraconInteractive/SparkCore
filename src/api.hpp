#pragma once
#include <httplib.h>
#include <memory>
namespace spark {
class DeviceStore;
class TimerStore;
class PulseStore;
class CalendarStore;
class DailyTaskStore;
class FileStorage;
void configure_api(httplib::Server& server, std::shared_ptr<DeviceStore> registry, std::shared_ptr<TimerStore> timers, int notification_port = 47684, std::shared_ptr<PulseStore> pulse = nullptr, std::shared_ptr<CalendarStore> calendar = nullptr, std::shared_ptr<DailyTaskStore> tasks = nullptr, std::shared_ptr<FileStorage> files = nullptr);
}
