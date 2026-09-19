#include "api.hpp"
#include "timers.hpp"
#include <mysql.h>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <array>
#include <algorithm>

using nlohmann::json;
void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
struct Database {
    MYSQL* connection;
    explicit Database(const spark::DatabaseConfig& c) : connection(mysql_init(nullptr)) {
        check(connection && mysql_real_connect(connection, c.host.c_str(), c.user.c_str(), c.password.c_str(),
              c.database.c_str(), c.port, c.socket.empty() ? nullptr : c.socket.c_str(), 0), "test database connection");
        run("SET time_zone='+00:00'");
    }
    ~Database() { mysql_close(connection); }
    void run(const std::string& sql) { check(mysql_query(connection, sql.c_str()) == 0, "test SQL statement"); }
    std::string scalar(const std::string& sql) {
        run(sql);
        auto result = std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)>(mysql_store_result(connection), mysql_free_result);
        check(bool(result), "test SQL result");
        auto row = mysql_fetch_row(result.get());
        check(row && row[0], "test SQL scalar");
        return row[0];
    }
};
json expect(const httplib::Result& response, int status, const char* error = nullptr) {
    check(response && response->status == status, "timer HTTP status");
    const auto body = json::parse(response->body);
    if (error) check(body["error"] == error, "timer error packet");
    return body;
}
int main() {
    const auto config = spark::DatabaseConfig::from_environment();
    if (config.database.rfind("spark_test_", 0) != 0) return 77;
    auto store = std::make_shared<spark::TimerStore>(config);
    store->verify();
    Database database(config);
    httplib::Server server;
    spark::configure_api(server, std::make_shared<spark::DeviceStore>(config), store);
    const int port = server.bind_to_any_port("127.0.0.1");
    if (port < 0) return 1;
    std::thread listener([&] { server.listen_after_bind(); });
    server.wait_until_ready();
    int result = 0;
    try {
        httplib::Client client("127.0.0.1", port);
        client.set_read_timeout(10, 0);
        const std::string missing = "/timers/00000000-0000-0000-0000-000000000000";
        expect(client.Get(missing), 404, "timer_not_found");
        expect(client.Post(missing + "/stop"), 404, "timer_not_found");
        expect(client.Get("/timers/bad"), 400, "invalid_timer_id");
        for (const std::string packet : {"{}", R"({"duration_seconds":0})", R"({"duration_seconds":-1})",
             R"({"duration_seconds":604801})", R"({"duration_seconds":1.5})", R"({"duration_seconds":true})",
             R"({"duration_seconds":18446744073709551615})"})
            expect(client.Post("/timers", packet, "application/json"), 400, "invalid_duration");
        expect(client.Post("/timers", "[]", "application/json"), 400, "invalid_packet");
        expect(client.Post("/timers", "{}", "text/plain"), 415, "unsupported_media_type");
        expect(client.Post("/timers", R"({"duration_seconds":60,"source_device":"bad.id"})", "application/json"), 400, "invalid_source_device");
        expect(client.Post("/timers", R"({"duration_seconds":60,"label":5})", "application/json"), 400, "invalid_label");
        expect(client.Post("/timers", R"({"duration_seconds":60,"extra":true})", "application/json"), 400, "unknown_field");
        auto created_response = client.Post("/timers", R"({"duration_seconds":60,"label":"Tea ☕","source_device":"watch-1"})", "application/json");
        const auto created = expect(created_response, 201);
        const auto id = created["id"].get<std::string>();
        const auto path = "/timers/" + id;
        check(created["status"] == "running" && created["remaining_seconds"] > 0 && created["remaining_seconds"] <= 60, "timer countdown");
        check(created["label"] == "Tea ☕" && created["source_device"] == "watch-1", "timer fields");
        check(created_response->get_header_value("Location") == path, "timer location");
        auto options = client.Options(path + "/confirm");
        check(options && options->status == 204 && options->get_header_value("Allow") == "POST, OPTIONS", "action preflight");
        expect(client.Delete(path), 405, "method_not_allowed");
        expect(client.Get(path + "/confirm"), 405, "method_not_allowed");
        auto head = client.Head(path);
        check(head && head->status == 200 && head->body.empty(), "timer HEAD");
        const auto active = expect(client.Get("/timers"), 200);
        check(active["timers"].size() == 1 && active["timers"][0]["id"] == id, "shared active list");
        expect(client.Post(path + "/confirm"), 409, "invalid_timer_state");
        expect(client.Post(path + "/stop", R"({"label":"change"})", "application/json"), 400, "unknown_field");
        auto stopped = expect(client.Post(path + "/stop", R"({"source_device":"phone-1"})", "application/json"), 200);
        check(stopped["status"] == "stopped" && stopped["remaining_seconds"] == 0, "stop timer");
        expect(client.Post(path + "/stop"), 200);
        expect(client.Post(path + "/confirm"), 409, "invalid_timer_state");
        check(expect(client.Get("/timers"), 200)["timers"].empty(), "stopped timer excluded");
        check(database.scalar("SELECT COUNT(*) FROM events WHERE resource='" + path + "'") == "2", "idempotent stop events");
        check(database.scalar("SELECT source_device FROM events WHERE resource='" + path + "' AND type='timer.stopped'") == "phone-1", "acting device event attribution");
        // Real time passes without HTTP polling; the worker must expire the timer.
        auto short_timer = expect(client.Post("/timers", R"({"duration_seconds":1})", "application/json"), 201);
        const auto short_id = short_timer["id"].get<std::string>();
        const auto short_path = "/timers/" + short_id;
        {
            spark::TimerScheduler scheduler(store);
            bool expired = false;
            for (int attempt = 0; attempt < 40; ++attempt) {
                if (database.scalar("SELECT status FROM timers WHERE id='" + short_id + "'") == "expired") { expired = true; break; }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            check(expired, "background expiry without requests");
        }
        auto expired = expect(client.Get(short_path), 200);
        check(expired["status"] == "expired" && expired["remaining_seconds"] == 0, "expired state");
        check(expect(client.Get("/timers"), 200)["timers"].size() == 1, "expired awaits acknowledgement");
        std::array<int, 6> statuses{};
        std::vector<std::thread> confirmers;
        for (std::size_t i = 0; i < statuses.size(); ++i) confirmers.emplace_back([&, i] {
            httplib::Client other("127.0.0.1", port);
            const auto response = other.Post(short_path + "/confirm");
            statuses[i] = response ? response->status : 0;
        });
        for (auto& thread : confirmers) thread.join();
        check(std::count(statuses.begin(), statuses.end(), 200) == 6, "concurrent idempotent confirmation");
        check(database.scalar("SELECT COUNT(*) FROM events WHERE resource='" + short_path + "'") == "3", "exactly one created expired confirmed event");
        expect(client.Post(short_path + "/stop"), 409, "invalid_timer_state");
        check(expect(client.Get("/timers"), 200)["timers"].empty(), "confirmed excluded");
        // Simulate a deadline passed while the service was down, then recover with a new store/worker.
        const auto restart_timer = store->create({{"duration_seconds", 60}});
        const auto restart_id = restart_timer["id"].get<std::string>();
        database.run("UPDATE timers SET ends_at=TIMESTAMPADD(SECOND,-10,CURRENT_TIMESTAMP(6)) WHERE id='" + restart_id + "'");
        auto fresh = std::make_shared<spark::TimerStore>(config);
        fresh->expire();
        fresh->expire();
        check((*fresh->get(restart_id))["status"] == "expired", "restart recovery");
        check(database.scalar("SELECT COUNT(*) FROM events WHERE resource='/timers/" + restart_id + "' AND type='timer.expired'") == "1", "expiry once across repeated workers");
        fresh->transition(restart_id, "stop", nullptr);
        // Inject an event failure and prove timer state cannot commit without its event.
        database.run("CREATE TRIGGER fail_timer_event BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test event failure'");
        expect(client.Post("/timers", R"({"duration_seconds":60,"label":"rollback"})", "application/json"), 503, "database_unavailable");
        check(database.scalar("SELECT COUNT(*) FROM timers WHERE label='rollback'") == "0", "create/event rollback");
        database.run("DROP TRIGGER fail_timer_event");
        auto rollback_timer = store->create({{"duration_seconds", 60}});
        const auto rollback_id = rollback_timer["id"].get<std::string>();
        database.run("CREATE TRIGGER fail_timer_event BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test event failure'");
        expect(client.Post("/timers/" + rollback_id + "/stop"), 503, "database_unavailable");
        check(database.scalar("SELECT status FROM timers WHERE id='" + rollback_id + "'") == "running", "transition/event rollback");
        database.run("UPDATE timers SET ends_at=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id='" + rollback_id + "'");
        bool failed = false;
        try { store->expire(); } catch (const spark::DatabaseError&) { failed = true; }
        check(failed && database.scalar("SELECT status FROM timers WHERE id='" + rollback_id + "'") == "running", "expiry/event rollback");
        database.run("DROP TRIGGER fail_timer_event");
        store->expire();
        check((*store->get(rollback_id))["status"] == "expired", "expiry recovers after database failure");
        std::cout << "Timer lifecycle, worker, recovery, concurrency, and atomic events passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        result = 1;
    }
    server.stop();
    listener.join();
    return result;
}
