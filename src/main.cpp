#include "api.hpp"
#include "config.hpp"
#include "commands.hpp"
#include "dashboard.hpp"
#include <thread>
#include <chrono>
#include "database.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include "pulse.hpp"
#include "calendar.hpp"
#include "daily_tasks.hpp"
#include "files.hpp"
#include "projects.hpp"
#include "boards.hpp"
#include <charconv>
#include <iostream>
#include <string>

int run_server(const spark::ServerConfig& config, std::shared_ptr<spark::Maintenance> maintenance) {
    const auto& host = config.host;
    const int port = config.port;
    const int notification_port = config.notification_port;
    httplib::Server server;
    std::shared_ptr<spark::CommandStore> commands;
    std::shared_ptr<spark::TimerStore> timers;
    std::shared_ptr<spark::NotificationStore> events;
    std::shared_ptr<spark::PulseStore> pulse;
    std::shared_ptr<spark::DailyTaskStore> tasks;
    std::shared_ptr<spark::CalendarStore> calendar;
    try {
        auto store = std::make_shared<spark::DeviceStore>(spark::DatabaseConfig::from_environment());
        store->verify();
        commands = std::make_shared<spark::CommandStore>(spark::DatabaseConfig::from_environment());
        commands->verify();
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
        auto files = std::make_shared<spark::FileStorage>(std::make_shared<spark::FileStore>(spark::DatabaseConfig::from_environment()),spark::FileStorage::default_roots());
        spark::ProjectStore(spark::DatabaseConfig::from_environment()).verify();
        spark::BoardStore(spark::DatabaseConfig::from_environment()).verify();
        files->initialize();
        spark::configure_api(server, std::move(store), timers, notification_port, pulse, calendar, tasks, files, maintenance);
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
    spark::CommandScheduler command_scheduler(commands);
    spark::TimerScheduler scheduler(timers);
    spark::PulseScheduler pulse_scheduler(pulse);
    spark::DailyTaskScheduler task_scheduler(tasks);
    spark::CalendarScheduler calendar_scheduler(calendar);
    std::cout << "TCP notifications listening on " << host << ':' << notifications->port() << std::endl;
    std::cout << "SPARK " << SPARK_VERSION << " listening on http://" << host << ':' << port << std::endl;
    std::atomic<bool> finished{false};
    std::thread controller([&] {
        while (!finished) {
            if (maintenance->requested) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                server.stop();
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });
    bool ok = server.listen_after_bind();
    finished = true;
    controller.join();
    return ok ? 0 : 1;
}

int main(int argc, char** argv) {
    spark::ServerConfig config;
    try {
        config = spark::ServerConfig::load(argc, argv);
        if (config.help) {
            std::cout << "Usage: spark_server [--config FILE] [--host ADDRESS] [--port PORT] [--notification-port PORT] [--check-config]\n"
                      << "--check-config validates without starting services; reports runtime blockers.\n";
            return 0;
        }
        if (config.check_only) {
            std::cout << config.summary().dump(2) << '\n';
            return 0;
        }
        config.require_supported_runtime();
        config.apply_environment();
    } catch (const std::exception& error) {
        std::cerr << "Configuration error: " << error.what() << '\n';
        return 2;
    }
    std::cout << "Authentication mode: open" << std::endl;
    auto maintenance = std::make_shared<spark::Maintenance>();
    for (;;) {
        int result = run_server(config, maintenance);
        if (!maintenance->requested) return result;
        // All HTTP handlers, schedulers, TCP clients and storage locks are now gone.
        try {
            maintenance->execute();
        } catch (const std::exception& error) {
            std::lock_guard<std::mutex> guard(maintenance->mutex);
            maintenance->result = {{"state", "failed"}, {"message", std::string("Maintenance failed: ") + error.what()}};
            maintenance->requested = 0;
        }
    }
}
