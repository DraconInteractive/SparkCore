#include "api.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include "pulse.hpp"
#include "calendar.hpp"
#include <mysql.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

using nlohmann::json;
using Clock = std::chrono::steady_clock;
void check(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
struct SQL {
    MYSQL* db;
    explicit SQL(const spark::DatabaseConfig& c) : db(mysql_init(nullptr)) {
        check(db && mysql_real_connect(db, c.host.c_str(), c.user.c_str(), c.password.c_str(), c.database.c_str(), c.port,
            c.socket.empty() ? nullptr : c.socket.c_str(), 0), "SQL connect");
        run("SET time_zone='+00:00'");
    }
    ~SQL() { mysql_close(db); }
    void run(const std::string& text) { check(mysql_query(db, text.c_str()) == 0, "SQL operation"); }
    unsigned long long scalar(const std::string& text) {
        run(text);
        auto result = std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)>(mysql_store_result(db), mysql_free_result);
        check(bool(result), "SQL result");
        auto row = mysql_fetch_row(result.get());
        check(row && row[0], "SQL scalar");
        return std::stoull(row[0]);
    }
};
struct Peer {
    int fd;
    std::string input;
    explicit Peer(int port) : fd(socket(AF_INET, SOCK_STREAM, 0)) {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
        check(fd >= 0 && connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "TCP connect");
    }
    ~Peer() { close(fd); }
    void send(const std::string& text) {
        std::size_t offset = 0;
        while (offset < text.size()) {
            auto count = ::send(fd, text.data() + offset, text.size() - offset, MSG_NOSIGNAL);
            check(count > 0, "TCP send");
            offset += count;
        }
    }
    json next(int timeout_ms = 5000) {
        const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
        for (;;) {
            const auto newline = input.find('\n');
            if (newline != std::string::npos) {
                auto packet = json::parse(input.substr(0, newline));
                input.erase(0, newline + 1);
                return packet;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
            check(remaining > 0, "TCP receive deadline");
            pollfd poller{fd, POLLIN, 0};
            check(poll(&poller, 1, static_cast<int>(remaining)) > 0, "TCP receive timeout");
            char buffer[4096];
            auto count = recv(fd, buffer, sizeof(buffer), 0);
            check(count > 0, "TCP disconnected unexpectedly");
            input.append(buffer, count);
        }
    }
    json until(const std::string& type, const std::string& resource = {}) {
        const auto deadline = Clock::now() + std::chrono::seconds(5);
        for (int i = 0; i < 100; ++i) {
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
            if (remaining <= 0) throw std::runtime_error("Timed out waiting for " + type + " " + resource);
            json packet;
            try { packet = next(static_cast<int>(remaining)); }
            catch (const std::exception& error) { throw std::runtime_error("Waiting for " + type + " " + resource + ": " + error.what()); }
            if (packet["type"] == "ping") send("{\"type\":\"pong\"}\n");
            if (packet["type"] == type && (resource.empty() || packet.value("resource", std::string()) == resource)) return packet;
        }
        throw std::runtime_error("Expected notification not found");
    }
    json hello(const std::string& id) {
        send(json{{"type", "hello"}, {"device_id", id}}.dump() + "\n");
        return until("welcome");
    }
    bool closed(int timeout_ms = 2000) {
        const auto end = Clock::now() + std::chrono::milliseconds(timeout_ms);
        while (Clock::now() < end) {
            pollfd poller{fd, POLLIN, 0};
            if (poll(&poller, 1, 100) > 0) {
                char buffer[4096];
                if (recv(fd, buffer, sizeof(buffer), 0) <= 0) return true;
            }
        }
        return false;
    }
};
int main() {
    auto config = spark::DatabaseConfig::from_environment();
    if (config.database.rfind("spark_test_", 0) != 0) return 77;
    try {
        auto events = std::make_shared<spark::NotificationStore>(config);
        auto timers = std::make_shared<spark::TimerStore>(config);
        std::cerr << "TCP test: setup\n";
        SQL database(config);
        // Sequence historical fixture events before accepting live test clients.
        events->sequence_events();
        spark::NotificationOptions options;
        options.host = "127.0.0.1";
        options.port = 0;
        options.heartbeat = std::chrono::seconds(1);
        options.idle_timeout = std::chrono::seconds(10);
        options.hello_timeout = std::chrono::milliseconds(500);
        auto notifications = std::make_unique<spark::NotificationServer>(events, options);
        bool collision = false;
        try {
            auto occupied = options;
            occupied.port = notifications->port();
            spark::NotificationServer duplicate(events, occupied);
        } catch (const std::exception&) { collision = true; }
        check(collision, "notification bind collision");
        std::cerr << "TCP test: handshake\n";
        Peer first(notifications->port()), second(notifications->port());
        // Split a hello across writes and combine another frame with its remainder.
        first.send("{\"type\":\"hel");
        first.send("lo\",\"device_id\":\"watch-1\"}\n{\"type\":\"ping\"}\n");
        auto welcome = first.until("welcome");
        check(welcome["resync_required"] == true && welcome["resources"][0] == "/timers", "welcome resync");
        first.until("pong");
        second.hello("phone-1");
        {
            Peer invalid(notifications->port());
            invalid.send("{broken}\n");
            check(invalid.next()["error"] == "invalid_message", "malformed JSON");
            check(invalid.closed(), "malformed peer disconnected");
        }
        {
            Peer invalid(notifications->port());
            invalid.send("{\"type\":\"hello\",\"device_id\":\"bad.id\"}\n");
            check(invalid.next()["error"] == "invalid_hello", "invalid device identity");
        }
        {
            Peer oversized(notifications->port());
            oversized.send(std::string(5000, 'x'));
            check(oversized.next()["error"] == "frame_too_large", "bounded input frame");
        }
        {
            Peer silent(notifications->port());
            check(silent.closed(), "hello deadline");
        }
        std::cerr << "TCP test: HTTP integration\n";
        httplib::Server http;
        spark::configure_api(http, std::make_shared<spark::DeviceStore>(config), timers, notifications->port());
        const auto http_port = http.bind_to_any_port("127.0.0.1");
        check(http_port > 0, "HTTP bind");
        std::thread http_thread([&] { http.listen_after_bind(); });
        http.wait_until_ready();
        try {
            httplib::Client client("127.0.0.1", http_port);
            std::cerr << "TCP test: discovery\n";
            auto discovery = client.Get("/api");
            check(discovery && json::parse(discovery->body)["notifications"]["port"] == notifications->port(), "configured TCP port discovery");
            std::cerr << "TCP test: creating timer\n";
            auto created = client.Post("/timers", R"({"duration_seconds":60,"source_device":"watch-1"})", "application/json");
            check(created && created->status == 201, "REST create timer");
            const auto timer = json::parse(created->body);
            const auto id = timer["id"].get<std::string>();
            const auto resource = "/timers/" + id;
            std::cerr << "TCP test: awaiting created broadcast\n";
            const auto a = first.until("timer.created", resource);
            const auto b = second.until("timer.created", resource);
            check(a == b && a["source_device"] == "watch-1", "broadcast including actor");
            std::cerr << "TCP test: fetching timer\n";
            auto fetched = client.Get(resource);
            check(fetched && fetched->status == 200 && json::parse(fetched->body)["status"] == "running", "notification after commit");
            check(database.scalar("SELECT COUNT(*) FROM notification_log WHERE event_id=" + a["event_id"].dump()) == 1, "durable event sequencing");
            std::cerr << "TCP test: scheduler expiry\n";
            // Exercise scheduler-driven notifications without timer GET requests.
            database.run("UPDATE timers SET ends_at=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id='" + id + "'");
            {
                spark::TimerScheduler scheduler(timers);
                first.until("timer.expired", resource);
                second.until("timer.expired", resource);
            }
            std::cerr << "TCP test: confirming timer\n";
            auto confirmed = client.Post(resource + "/confirm");
            check(confirmed && confirmed->status == 200, "REST confirm timer");
            first.until("timer.confirmed", resource);
            second.until("timer.confirmed", resource);
            auto calendar = std::make_shared<spark::CalendarStore>(config);
            auto dated = calendar->create({{"name", "TCP fixture"}, {"description", "Notification test"}, {"occurs_at", "2026-09-19T00:00:00Z"}});
            const auto dated_id = dated["id"].get<std::string>();
            first.until("calendar.created", "/events/" + dated_id);
            second.until("calendar.created", "/events/" + dated_id);
            calendar->patch(dated_id, {{"name", "Changed"}});
            first.until("calendar.updated", "/events/" + dated_id);
            second.until("calendar.updated", "/events/" + dated_id);
            calendar->remove(dated_id);
            first.until("calendar.deleted", "/events/" + dated_id);
            second.until("calendar.deleted", "/events/" + dated_id);
            std::cerr << "TCP test: pulse lifecycle and broadcast\n";
            auto pulse = std::make_shared<spark::PulseStore>(config);
            Peer never_subscribed(notifications->port());
            never_subscribed.hello("Watch-1"); // IDs are case-sensitive.
            auto subscribe = [&](const std::string& id) {
                auto response = client.Post("/pulse/subscriptions/" + id);
                check(response && response->status == 200 && json::parse(response->body) == json{{"device_id", id}, {"subscribed", true}}, "subscribe response");
            };
            auto unsubscribe = [&](const std::string& id) {
                auto response = client.Delete("/pulse/subscriptions/" + id);
                check(response && response->status == 200 && json::parse(response->body) == json{{"device_id", id}, {"subscribed", false}}, "unsubscribe response");
            };
            auto barrier = [&](const std::string& name) {
                database.run("INSERT INTO events(type,resource,payload) VALUES ('test.barrier','/barrier/" + name + "','{}')");
            };
            auto no_pulse_until = [&](Peer& peer, const std::string& name) {
                for (int i = 0; i < 100; ++i) {
                    auto packet = peer.next();
                    const std::string type = packet["type"];
                    check(type.rfind("pulse.", 0) != 0, "unsubscribed client must not receive pulse events");
                    if (type == "ping") peer.send("{\"type\":\"pong\"}\n");
                    if (type == "test.barrier" && packet["resource"] == "/barrier/" + name) return;
                }
                throw std::runtime_error("Missing subscription test barrier");
            };
            subscribe("watch-1"); subscribe("watch-1"); subscribe("phone-1");
            check(database.scalar("SELECT COUNT(*) FROM pulse_subscriptions WHERE device_id='watch-1'") == 1, "idempotent subscription");
            check(std::make_shared<spark::NotificationStore>(config)->pulse_subscribers().count("watch-1") == 1, "persistent subscription");
            auto bad_subscription = client.Post("/pulse/subscriptions/bad.id");
            check(bad_subscription && bad_subscription->status == 400, "invalid subscription ID");
            auto subscription_body = client.Post("/pulse/subscriptions/watch-1", "{}", "application/json");
            check(subscription_body && subscription_body->status == 400, "subscription body rejected");
            auto subscription_options = client.Options("/pulse/subscriptions/watch-1");
            check(subscription_options && subscription_options->status == 204 && subscription_options->get_header_value("Allow") == "POST, DELETE, OPTIONS", "subscription preflight");
            unsubscribe("not-subscribed");

            pulse->verify();
            auto initial_response = client.Get("/pulse");
            check(initial_response && initial_response->status == 200, "pulse GET");
            auto initial = json::parse(initial_response->body);
            check(initial["enabled"] == false && initial["interval_seconds"] == 3600 && initial["next_pulse_at"].is_null(), "disabled pulse defaults");
            pulse->tick();
            check(pulse->get()["pulse_count"] == 0, "disabled pulse does not fire");
            for (const std::string packet : {"{}", "[]", R"({"enabled":1})", R"({"interval_seconds":0})", R"({"interval_seconds":-1})",
                    R"({"interval_seconds":1.5})", R"({"interval_seconds":604801})", R"({"interval_seconds":18446744073709551615})",
                    R"({"enabled":true,"extra":true})", R"({"enabled":true,"source_device":"bad.id"})"}) {
                auto invalid = client.Patch("/pulse", packet, "application/json");
                check(invalid && invalid->status == 400, "pulse validation");
            }
            auto media = client.Patch("/pulse", "{}", "text/plain");
            check(media && media->status == 415, "pulse content type");
            auto method = client.Post("/pulse");
            check(method && method->status == 405, "pulse allowed methods");
            auto preflight = client.Options("/pulse");
            check(preflight && preflight->status == 204 && preflight->get_header_value("Allow").find("PATCH") != std::string::npos, "pulse preflight");
            auto enable = client.Patch("/pulse", R"({"enabled":true,"interval_seconds":1,"source_device":"phone-1"})", "application/json");
            check(enable && enable->status == 200 && json::parse(enable->body)["enabled"] == true, "enable pulse");
            check(first.until("pulse.updated", "/pulse")["source_device"] == "phone-1", "pulse settings broadcast attribution");
            {
                spark::PulseScheduler scheduler(pulse);
                auto pulse_a = first.until("pulse.triggered", "/pulse");
                auto pulse_b = second.until("pulse.triggered", "/pulse");
                check(pulse_a == pulse_b && pulse_a["source_device"].is_null(), "pulse broadcast to all clients");
            }
            barrier("initial");
            first.until("test.barrier", "/barrier/initial");
            second.until("test.barrier", "/barrier/initial");
            no_pulse_until(never_subscribed, "initial");
            unsubscribe("phone-1"); unsubscribe("phone-1");
            pulse->patch({{"interval_seconds", 3600}});
            database.run("UPDATE pulse SET next_pulse_at=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id=1");
            pulse->tick();
            first.until("pulse.triggered", "/pulse");
            barrier("unsubscribed");
            no_pulse_until(second, "unsubscribed");
            no_pulse_until(never_subscribed, "unsubscribed");
            // Timer notifications still reach an unsubscribed device.
            auto ordinary_timer = timers->create({{"duration_seconds", 60}});
            second.until("timer.created", "/timers/" + ordinary_timer["id"].get<std::string>());
            subscribe("phone-1");
            {
                Peer reconnected_subscription(notifications->port());
                reconnected_subscription.hello("phone-1");
                database.run("UPDATE pulse SET next_pulse_at=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id=1");
                pulse->tick();
                reconnected_subscription.until("pulse.triggered", "/pulse");
                first.until("pulse.triggered", "/pulse");
                second.until("pulse.triggered", "/pulse");
            }
            auto state = pulse->get();
            check(state["pulse_count"] >= 1 && !state["last_pulse_at"].is_null(), "background pulse persisted");
            pulse->patch({{"interval_seconds", 3600}});
            state = pulse->get();
            check(pulse->patch({{"enabled", true}, {"interval_seconds", 3600}}) == state, "identical pulse settings preserve deadline");
            const auto baseline = state["pulse_count"].get<unsigned long long>();
            database.run("UPDATE pulse SET next_pulse_at=TIMESTAMPADD(DAY,-2,CURRENT_TIMESTAMP(6)) WHERE id=1");
            std::array<std::exception_ptr, 6> pulse_failures{};
            std::vector<std::thread> pulse_workers;
            for (std::size_t i = 0; i < pulse_failures.size(); ++i) pulse_workers.emplace_back([&, i] {
                try { pulse->tick(); } catch (...) { pulse_failures[i] = std::current_exception(); }
            });
            for (auto& worker : pulse_workers) worker.join();
            for (const auto& failure : pulse_failures) check(!failure, "concurrent pulse scheduler");
            check(pulse->get()["pulse_count"] == baseline + 1, "missed intervals coalesced across concurrent workers");
            pulse->tick();
            check(pulse->get()["pulse_count"] == baseline + 1, "no catchup burst");
            auto fresh_pulse = std::make_shared<spark::PulseStore>(config);
            check(fresh_pulse->get() == pulse->get(), "pulse restart persistence");
            // Neither settings nor due pulses may commit without their event.
            database.run("CREATE TRIGGER reject_pulse_event BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.resource='/pulse' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='pulse test failure'; END IF; END");
            const auto before_failure = pulse->get();
            auto rejected = client.Patch("/pulse", R"({"enabled":false})", "application/json");
            check(rejected && rejected->status == 503 && pulse->get() == before_failure, "pulse settings/event rollback");
            database.run("UPDATE pulse SET next_pulse_at=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id=1");
            bool pulse_failed = false;
            try { pulse->tick(); } catch (const spark::DatabaseError&) { pulse_failed = true; }
            check(pulse_failed && pulse->get()["pulse_count"] == baseline + 1, "pulse trigger/event rollback");
            database.run("DROP TRIGGER reject_pulse_event");
            pulse->tick();
            check(pulse->get()["pulse_count"] == baseline + 2, "pulse recovers after event failure");
            auto disable = client.Patch("/pulse", R"({"enabled":false})", "application/json");
            check(disable && disable->status == 200, "disable pulse");
            auto disabled = json::parse(disable->body);
            check(disabled["enabled"] == false && disabled["next_pulse_at"].is_null(), "disable clears deadline");
            pulse->tick();
            check(pulse->get() == disabled, "disabled schedule stays idle");
            std::cerr << "TCP test: commit ordering\n";
            // Late lower event ID must receive a *later* dispatch sequence.
            SQL slow(config), fast(config);
            slow.run("START TRANSACTION");
            slow.run("INSERT INTO events(type,resource,payload) VALUES ('test.late','/test/late','{}')");
            auto low = mysql_insert_id(slow.db);
            fast.run("INSERT INTO events(type,resource,payload) VALUES ('test.early','/test/early','{}')");
            auto high = mysql_insert_id(fast.db);
            check(high > low, "event ID ordering fixture");
            std::cerr << "TCP test: awaiting high event\n";
            auto early = first.until("test.early");
            check(early["event_id"] == high, "committed higher event delivered");
            std::cerr << "TCP test: committing low event\n";
            slow.run("COMMIT");
            auto late = first.until("test.late");
            check(late["event_id"] == low && late["sequence"] > early["sequence"], "late commit is not skipped");
            std::cerr << "TCP test: rollback\n";
            // Rolled-back event must never enter notification_log.
            slow.run("START TRANSACTION");
            slow.run("INSERT INTO events(type,resource,payload) VALUES ('test.rollback','/test/rollback','{}')");
            const auto rolled_back = mysql_insert_id(slow.db);
            slow.run("ROLLBACK");
            events->sequence_events();
            check(database.scalar("SELECT COUNT(*) FROM notification_log WHERE event_id=" + std::to_string(rolled_back)) == 0, "no rolled-back notifications");
            std::cerr << "TCP test: concurrent dispatchers\n";
            // Pollers can run concurrently without duplicating sequence assignments.
            std::exception_ptr failure1, failure2;
            std::thread poller1([&] { try { events->sequence_events(); } catch (...) { failure1 = std::current_exception(); } });
            std::thread poller2([&] { try { events->sequence_events(); } catch (...) { failure2 = std::current_exception(); } });
            poller1.join(); poller2.join();
            check(!failure1 && !failure2, "concurrent dispatchers");
            check(database.scalar("SELECT COUNT(*) FROM notification_log WHERE event_id=" + std::to_string(low)) == 1, "one sequence per event");
            std::cerr << "TCP test: reconnect\n";
            // A new TCP session always asks for REST resync, covering offline changes.
            {
                Peer resumed(notifications->port());
                check(resumed.hello("watch-1")["resync_required"] == true, "reconnect resync");
                auto active = client.Get("/timers");
                check(active && active->status == 200, "reconnect REST refresh");
                resumed.until("ping");
                resumed.send("{\"type\":\"pong\"}\n{\"type\":\"ping\"}\n");
                resumed.until("pong");
            }
        } catch (...) {
            http.stop(); http_thread.join(); throw;
        }
        http.stop(); http_thread.join();
        std::cerr << "TCP test: shutdown\n";
        // Simulate dispatch storage loss, verify disconnect, then restore and resume.
        database.run("RENAME TABLE notification_log TO notification_log_offline");
        check(first.closed(), "database outage forces client resync");
        database.run("RENAME TABLE notification_log_offline TO notification_log");
        {
            Peer recovered(notifications->port());
            check(recovered.hello("recovered-1")["resync_required"] == true, "database recovery welcome");
            auto recovered_timer = timers->create({{"duration_seconds", 60}});
            recovered.until("timer.created", "/timers/" + recovered_timer["id"].get<std::string>());
        }
        notifications.reset();
        check(second.closed(), "shutdown closes clients");
        // Restart publisher; new committed events still broadcast.
        notifications = std::make_unique<spark::NotificationServer>(events, options);
        Peer restarted(notifications->port());
        restarted.hello("phone-1");
        auto timer = timers->create({{"duration_seconds", 60}});
        restarted.until("timer.created", "/timers/" + timer["id"].get<std::string>());
        notifications.reset();
        options.heartbeat = std::chrono::milliseconds(100);
        options.idle_timeout = std::chrono::milliseconds(500);
        notifications = std::make_unique<spark::NotificationServer>(events, options);
        Peer idle(notifications->port());
        idle.hello("idle-1");
        check(idle.closed(), "unresponsive heartbeat client disconnected");
        std::cout << "TCP framing, broadcast, timer integration, reconnect, heartbeats, and commit ordering passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
    return 0;
}
