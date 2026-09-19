#include "api.hpp"
#include "database.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include "pulse.hpp"
#include "calendar.hpp"
#include "daily_tasks.hpp"
#include "files.hpp"
#include <charconv>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
    std::string host = "0.0.0.0";
    int port = 47683;
    int notification_port = 47684;
    for (int i = 1; i < argc; ++i) {
        const std::string option = argv[i];
        if (option == "--help") {
            std::cout << "Usage: spark_server [--host ADDRESS] [--port PORT] [--notification-port PORT]\n";
            return 0;
        }
        if ((option != "--host" && option != "--port" && option != "--notification-port") || i + 1 == argc) {
            std::cerr << "Expected --host ADDRESS, --port PORT, or --notification-port PORT\n";
            return 2;
        }
        const std::string value = argv[++i];
        if (option == "--host") host = value;
        else {
            auto& selected_port = option == "--port" ? port : notification_port;
            const auto result = std::from_chars(value.data(), value.data() + value.size(), selected_port);
            if (result.ec != std::errc{} || result.ptr != value.data() + value.size() || selected_port < 1 || selected_port > 65535) {
                std::cerr << "Port must be an integer from 1 to 65535\n";
                return 2;
            }
        }
    }
    httplib::Server server;
    std::shared_ptr<spark::TimerStore> timers;
    std::shared_ptr<spark::NotificationStore> events;
    std::shared_ptr<spark::PulseStore> pulse;
    std::shared_ptr<spark::DailyTaskStore> tasks;
    std::shared_ptr<spark::CalendarStore> calendar;
    try {
        auto store = std::make_shared<spark::DeviceStore>(spark::DatabaseConfig::from_environment());
        store->verify();
        timers = std::make_shared<spark::TimerStore>(spark::DatabaseConfig::from_environment());
        timers->verify();
        events = std::make_shared<spark::NotificationStore>(spark::DatabaseConfig::from_environment());
        events->verify();
        pulse = std::make_shared<spark::PulseStore>(spark::DatabaseConfig::from_environment());
        pulse->verify();
        calendar = std::make_shared<spark::CalendarStore>(spark::DatabaseConfig::from_environment());
        calendar->verify();
        tasks = std::make_shared<spark::DailyTaskStore>(spark::DatabaseConfig::from_environment());
        tasks->verify();
        auto files = std::make_shared<spark::FileStorage>(std::make_shared<spark::FileStore>(spark::DatabaseConfig::from_environment()),spark::FileStorage::default_root());
        files->initialize();
        spark::configure_api(server, std::move(store), timers, notification_port, pulse, calendar, tasks, files);
    } catch (const std::exception& error) {
        std::cerr << "Startup check failed: " << error.what() << '\n';
        return 1;
    }
    if (!server.bind_to_port(host, port)) {
        std::cerr << "Cannot bind to " << host << ':' << port << '\n';
        return 1;
    }
    std::unique_ptr<spark::NotificationServer> notifications;
    try {
        spark::NotificationOptions options;
        options.host = host;
        options.port = notification_port;
        notifications = std::make_unique<spark::NotificationServer>(events, options);
    } catch (const std::exception& error) {
        std::cerr << "Notification startup failed: " << error.what() << '\n';
        return 1;
    }
    spark::TimerScheduler scheduler(timers);
    spark::PulseScheduler pulse_scheduler(pulse);
    spark::DailyTaskScheduler task_scheduler(tasks);
    spark::CalendarScheduler calendar_scheduler(calendar);
    std::cout << "TCP notifications listening on " << host << ':' << notifications->port() << std::endl;
    std::cout << "SPARK " << SPARK_VERSION << " listening on http://" << host << ':' << port << std::endl;
    return server.listen_after_bind() ? 0 : 1;
}
