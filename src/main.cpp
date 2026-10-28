#include "configs.hpp"
#include "logging.hpp"
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

int run_server(const spark::ServerConfig& config, std::shared_ptr<spark::Maintenance> maintenance,std::shared_ptr<spark::Authorization> auth) {
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
    std::string startup_check="devices";
    try {
        auto store = std::make_shared<spark::DeviceStore>(spark::DatabaseConfig::from_environment());
        store->verify();
        commands = std::make_shared<spark::CommandStore>(spark::DatabaseConfig::from_environment());
        startup_check="commands"; commands->verify();
        timers = std::make_shared<spark::TimerStore>(spark::DatabaseConfig::from_environment());
        startup_check="timers"; timers->verify();
        events = std::make_shared<spark::NotificationStore>(spark::DatabaseConfig::from_environment());
        startup_check="notification_log"; events->verify();
        pulse = std::make_shared<spark::PulseStore>(spark::DatabaseConfig::from_environment());
        startup_check="pulse"; pulse->verify();
        calendar = std::make_shared<spark::CalendarStore>(spark::DatabaseConfig::from_environment());
        startup_check="calendar"; calendar->verify();
        tasks = std::make_shared<spark::DailyTaskStore>(spark::DatabaseConfig::from_environment());
        startup_check="daily_tasks"; tasks->verify();
        startup_check="configs"; spark::ConfigStore(spark::DatabaseConfig::from_environment()).verify();
        auto files = std::make_shared<spark::FileStorage>(std::make_shared<spark::FileStore>(spark::DatabaseConfig::from_environment()),spark::FileStorage::default_roots());
        startup_check="projects"; spark::ProjectStore(spark::DatabaseConfig::from_environment()).verify();
        startup_check="boards"; spark::BoardStore(spark::DatabaseConfig::from_environment()).verify();
        startup_check="storage"; files->initialize();
        try{maintenance->initialize();}catch(const std::exception& e){spark::log_exception("warning","backup","Backup visibility unavailable",e);}
        spark::configure_api(server, std::move(store), timers, notification_port, pulse, calendar, tasks, files, maintenance, auth, config.public_notifications());
    } catch (const std::exception& error) {
        spark::log_exception("error","server","Startup check failed",error,{{"check",startup_check}});
        return 1;
    }
    if (!server.bind_to_port(host, port)) {
        spark::log_event("error","http","Cannot bind listener",{{"port",port}});
        return 1;
    }
    std::unique_ptr<spark::NotificationServer> notifications;
    try {
        spark::NotificationOptions options;
        options.host = host;
        options.port = notification_port;
        if(auth)options.identity_provider=auth->provider();
        notifications = std::make_unique<spark::NotificationServer>(events, options);
    } catch (const std::exception& error) {
        spark::log_exception("error","notification","Notification startup failed",error);
        return 1;
    }
    spark::CommandScheduler command_scheduler(commands);
    spark::TimerScheduler scheduler(timers);
    spark::PulseScheduler pulse_scheduler(pulse);
    spark::DailyTaskScheduler task_scheduler(tasks);
    spark::CalendarScheduler calendar_scheduler(calendar);
    spark::log_event("info","notification","TCP notification listener started",{{"host",host},{"port",notifications->port()}});
    spark::log_event("info","http","HTTP listener started",{{"host",host},{"port",port},{"version",SPARK_VERSION}});
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
    spark::log_event(ok?"info":"error","server","HTTP listener stopped",{{"maintenance_requested",bool(maintenance->requested)}});
    return ok ? 0 : 1;
}

int main(int argc, char** argv) {
    spark::ServerConfig config;
    std::shared_ptr<spark::Authorization> auth;
    try {
        config = spark::ServerConfig::load(argc, argv);
        if (config.help) {
            std::cout << "Usage: spark_server [--config FILE] [--host ADDRESS] [--port PORT] [--notification-port PORT] [--check-config] [--remote]\n"
                      << "--remote requires authenticated, loopback-only TLS proxy configuration.\n"
                      << "--check-config validates without starting services; reports runtime blockers.\n";
            return 0;
        }
        if (config.check_only) {
            std::cout << config.summary().dump(2) << '\n';
            return 0;
        }
        config.require_supported_runtime();
        config.apply_environment();
        if(config.auth)auth=std::make_shared<spark::Authorization>(std::make_shared<spark::TokenIdentityProvider>(config.auth->credentials_file),config.auth->browser_origin);
    } catch (const std::exception& error) {
        if(dynamic_cast<const spark::ConfigError*>(&error))spark::log_event("error","config","Configuration error",{{"reason",error.what()}});
        else spark::log_exception("error","config","Configuration or credential validation failed",error);
        return 2;
    }
    spark::log_event("info","auth","Authentication configured",{{"mode",config.auth?"tokens":"open"}});
    auto maintenance = std::make_shared<spark::Maintenance>();
    for (;;) {
        int result = run_server(config, maintenance,auth);
        if (!maintenance->requested) return result;
        // All HTTP handlers, schedulers, TCP clients and storage locks are now gone.
        try {
            if(maintenance->requested==2)spark::run_maintenance_listener(config.host,config.port,maintenance,auth);
            else maintenance->execute();
        } catch (const std::exception& error) {
            spark::log_exception("error","maintenance","Maintenance failed",error);
            try{maintenance->progress({{"state","failed"},{"message",error.what()},{"finished_at",time(nullptr)},{"process_alive",false}});}catch(...){}
            std::lock_guard<std::mutex> guard(maintenance->mutex);
            maintenance->result = {{"state", "failed"}, {"message", std::string("Maintenance failed: ") + error.what()}};
            maintenance->requested = 0;
        }
    }
}
