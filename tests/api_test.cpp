#include "api.hpp"
#include "database.hpp"
#include "timers.hpp"
#include <nlohmann/json.hpp>
#include <iostream>
#include <cstdlib>
#include <algorithm>
#include <array>
#include <vector>
#include <stdexcept>
#include <thread>

void check(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
int main() {
    httplib::Server server;
    auto config = spark::DatabaseConfig::from_environment();
    if (config.database.rfind("spark_test_", 0) != 0) {
        std::cerr << "Use scripts/test-db.sh or an isolated SPARK_DB_NAME=spark_test_* database\n";
        return 77;
    }
    auto store = std::make_shared<spark::DeviceStore>(config);
    store->verify();
    spark::configure_api(server, store, std::make_shared<spark::TimerStore>(config));
    const int port = server.bind_to_any_port("127.0.0.1");
    if (port < 0) { std::cerr << "Cannot bind test listener\n"; return 1; }
    std::thread worker([&] { server.listen_after_bind(); });
    server.wait_until_ready();
    int result = 0;
    try {
        httplib::Client client("127.0.0.1", port);
        client.set_read_timeout(3, 0);
        auto health = client.Get("/health");
        check(health && health->status == 200, "health status");
        check(nlohmann::json::parse(health->body) == nlohmann::json{{"status", "ok"}}, "health packet");
        auto api = client.Get("/api");
        check(api && api->status == 200, "discovery status");
        auto document = nlohmann::json::parse(api->body);
        check(document["authentication"] == "none", "authentication");
        check(document["endpoints"].size() == 122, "endpoint count");
        for (const auto& endpoint : document["endpoints"]) {
            if(endpoint["path"].get<std::string>().rfind("/calendar-sync",0)==0 || endpoint.contains("access") || endpoint["path"].get<std::string>().rfind("/backups",0)==0 || endpoint["path"].get<std::string>().rfind("/connections",0)==0)continue;
            if (endpoint["path"].get<std::string>().find("/commands") == 0 || endpoint["path"] == "/devices/{id}/actions" || endpoint["path"] == "/" || endpoint["path"].get<std::string>().find("/admin/") == 0 || endpoint["path"] == "/project/{id}/files" || endpoint["path"].get<std::string>().find("/projects") == 0 || endpoint["path"].get<std::string>().find("/files") == 0 || endpoint["path"] == "/api/docs" || endpoint["path"].get<std::string>().find("/daily-tasks") == 0 || endpoint["path"].get<std::string>().find("/events") == 0 || endpoint["method"] != "GET" || endpoint["path"] == "/pulse" || endpoint["path"].get<std::string>().find("/timers") == 0 || endpoint["path"] == "/devices/{id}") continue;
            auto response = client.Get(endpoint["path"].get<std::string>());
            check(response && response->status == endpoint["output"]["status"], "documented status");
            check(response->get_header_value("Content-Type") == "application/json", "JSON content type");
            check(response->get_header_value("Cache-Control") == "no-store", "fresh polls");
            check(response->get_header_value("Access-Control-Allow-Origin") == "*", "browser access");
            const auto body = nlohmann::json::parse(response->body);
            for (const auto& field : endpoint["output"]["fields"].items())
                check(body.contains(field.key()), "documented field missing");
            if (endpoint["path"] != "/api") check(body == endpoint["output"]["body"], "documented packet");
            auto head = client.Head(endpoint["path"].get<std::string>());
            check(head && head->status == 200 && head->body.empty(), "HEAD support");
            auto options = client.Options(endpoint["path"].get<std::string>());
            check(options && options->status == 204, "OPTIONS support");
        }
        for(const auto* path:{"/admin/credentials","/admin/credentials/explorer"}){
            auto disabled=client.Get(path);
            check(disabled && disabled->status==409,"credential management disabled in open mode");
        }
        auto docs = client.Get("/api/docs");
        check(docs && docs->status == 200 && docs->get_header_value("Content-Type") == "text/html; charset=utf-8", "HTML docs route");
        check(docs->body.find("<nav id=\"endpoints\"") != std::string::npos && docs->body.find("fetch('/api'") != std::string::npos, "docs use live discovery");
        auto docs_head = client.Head("/api/docs");
        check(docs_head && docs_head->status == 200 && docs_head->body.empty(), "docs HEAD");
        auto docs_options = client.Options("/api/docs");
        check(docs_options && docs_options->status == 204, "docs OPTIONS");
        auto docs_post = client.Post("/api/docs", "", "text/plain");
        check(docs_post && docs_post->status == 405, "docs method restriction");
        auto version = nlohmann::json::parse(client.Get("/version")->body);
        check(version["server_version"] == SPARK_VERSION && version["api_version"] == document["api_version"], "version consistency");
        auto missing = client.Get("/missing");
        check(missing && missing->status == 404 && nlohmann::json::parse(missing->body)["error"] == "not_found", "404 packet");
        auto post = client.Post("/health", "", "application/json");
        check(post && post->status == 405 && post->get_header_value("Allow") == "GET, HEAD, OPTIONS", "405 response");
        check(nlohmann::json::parse(post->body)["error"] == "method_not_allowed", "405 packet");
        auto query = client.Get("/health?device=watch");
        check(query && query->status == 200, "query string routing");
        auto expect = [](const httplib::Result& response, int status, const char* error = nullptr) {
            check(response && response->status == status, "device response status");
            if (error) check(nlohmann::json::parse(response->body)["error"] == error, "device error packet");
        };
        auto created = client.Post("/devices/watch-1", R"({"id":"watch-1","local_ip":"192.168.1.50"})", "application/json");
        expect(created, 201);
        check(nlohmann::json::parse(created->body) == nlohmann::json{
            {"id", "watch-1"}, {"local_ip", "192.168.1.50"}, {"external_ip", nullptr}}, "created device");
        auto details = client.Get("/devices/watch-1");
        expect(details, 200);
        auto expected_details = nlohmann::json::parse(created->body);
        expected_details["subscribed"] = false;
        check(nlohmann::json::parse(details->body) == expected_details, "GET device details with default subscription");
        check(details->get_header_value("Content-Type") == "application/json" &&
              details->get_header_value("Cache-Control") == "no-store", "GET response headers");
        for (const auto& endpoint : document["endpoints"])
            if (endpoint["method"] == "GET" && endpoint["path"] == "/devices/{id}") {
                check(endpoint["input"]["body"].is_null() && endpoint["input"]["headers"].empty(), "GET discovery input");
                check(endpoint["output"]["body"] == nlohmann::json::parse(details->body), "GET discovery packet");
            }
        expect(client.Post("/pulse/subscriptions/watch-1"), 200);
        auto subscribed = client.Get("/devices/watch-1");
        expect(subscribed, 200);
        auto subscribed_body = nlohmann::json::parse(subscribed->body);
        check(subscribed_body["subscribed"].is_boolean() && subscribed_body["subscribed"] == true, "GET reflects subscribe");
        expect(client.Get("/devices/Watch-1"), 404, "device_not_found");
        expect(client.Delete("/pulse/subscriptions/watch-1"), 200);
        auto unsubscribed = client.Get("/devices/watch-1");
        expect(unsubscribed, 200);
        check(nlohmann::json::parse(unsubscribed->body)["subscribed"] == false, "GET reflects unsubscribe");
        expect(client.Post("/pulse/subscriptions/unregistered"), 200);
        expect(client.Get("/devices/unregistered"), 404, "device_not_found");
        expect(client.Delete("/pulse/subscriptions/unregistered"), 200);
        auto device_head = client.Head("/devices/watch-1");
        expect(device_head, 200);
        check(device_head->body.empty(), "device HEAD response");
        expect(client.Get("/devices/missing"), 404, "device_not_found");
        expect(client.Get("/devices/Watch-1"), 404, "device_not_found");
        expect(client.Get("/devices/bad.id"), 400, "invalid_id");
        expect(client.Post("/devices/watch-1", R"({"local_ip":"192.168.1.51"})", "application/json"), 409, "device_exists");
        auto patched = client.Patch("/devices/watch-1", R"({"external_ip":"203.0.113.5"})", "application/json");
        expect(patched, 200);
        auto record = nlohmann::json::parse(patched->body);
        check(record["local_ip"] == "192.168.1.50" && record["external_ip"] == "203.0.113.5", "partial update preserves local IP");
        expect(client.Patch("/devices/watch-1", R"({"local_ip":"bad","external_ip":null})", "application/json"), 400, "invalid_ip");
        patched = client.Patch("/devices/watch-1", R"({"local_ip":"fd00::1"})", "application/json");
        expect(patched, 200);
        record = nlohmann::json::parse(patched->body);
        details = client.Get("/devices/watch-1");
        expect(details, 200);
        auto expected_updated = record;
        expected_updated["subscribed"] = false;
        check(nlohmann::json::parse(details->body) == expected_updated, "GET reflects updated IPv6 and external IP");
        check(record["local_ip"] == "fd00::1" && record["external_ip"] == "203.0.113.5", "IPv6 and rejected update is atomic");
        patched = client.Patch("/devices/watch-1", R"({"external_ip":null})", "application/json");
        expect(patched, 200);
        check(nlohmann::json::parse(patched->body)["external_ip"].is_null(), "clear external IP");
        for (const std::string packet : {"[]", "null", "{broken"})
            expect(client.Post("/devices/invalid", packet, "application/json"), 400, "invalid_packet");
        expect(client.Post("/devices/invalid", "{}", "application/json"), 400, "missing_fields");
        expect(client.Patch("/devices/watch-1", "{}", "application/json"), 400, "missing_fields");
        expect(client.Post("/devices/invalid", R"({"local_ip":null})", "application/json"), 400, "invalid_ip");
        expect(client.Post("/devices/invalid", R"({"local_ip":"999.1.1.1"})", "application/json"), 400, "invalid_ip");
        expect(client.Post("/devices/invalid", R"({"local_ip":"192.168.1.5","external_ip":42})", "application/json"), 400, "invalid_ip");
        expect(client.Post("/devices/invalid", R"({"local_ip":"192.168.1.5","id":"other"})", "application/json"), 400, "id_mismatch");
        expect(client.Post("/devices/invalid", R"({"local_ip":"192.168.1.5","extra":true})", "application/json"), 400, "unknown_field");
        expect(client.Post("/devices/invalid", "{}", "text/plain"), 415, "unsupported_media_type");
        expect(client.Post("/devices/bad.id", "{}", "application/json"), 400, "invalid_id");
        expect(client.Patch("/devices/missing", R"({"local_ip":"192.168.1.5"})", "application/json"), 404, "device_not_found");
        auto options = client.Options("/devices/watch-1", {{"Origin", "http://device"}, {"Access-Control-Request-Method", "PATCH"}, {"Access-Control-Request-Headers", "content-type"}});
        expect(options, 204);
        check(options->get_header_value("Access-Control-Allow-Headers") == "Content-Type", "JSON CORS preflight");
        auto wrong_method = client.Put("/devices/watch-1", "", "application/json");
        expect(wrong_method, 405, "method_not_allowed");
        check(wrong_method->get_header_value("Allow") == "GET, HEAD, POST, PATCH, DELETE, OPTIONS", "device allowed methods");
        auto removed = client.Delete("/devices/watch-1");
        expect(removed, 204);
        check(removed->body.empty(), "delete has no body");
        expect(client.Get("/devices/watch-1"), 404, "device_not_found");
        expect(client.Delete("/devices/watch-1"), 404, "device_not_found");
        expect(client.Post("/devices/watch-1", R"({"local_ip":"192.168.1.50","external_ip":null})", "application/json"), 201);
        // Forwarded headers must not turn a LAN/loopback connection into an inferred public IP.
        auto forwarded = client.Post("/devices/forwarded", {{"X-Forwarded-For", "8.8.8.8"}}, R"({"local_ip":"192.168.1.51"})", "application/json");
        expect(forwarded, 201);
        check(nlohmann::json::parse(forwarded->body)["external_ip"].is_null(), "ignore forwarded headers");
        std::array<int, 8> statuses{};
        std::vector<std::thread> racers;
        for (std::size_t i = 0; i < statuses.size(); ++i) racers.emplace_back([&, i] {
            httplib::Client concurrent("127.0.0.1", port);
            auto response = concurrent.Post("/devices/race", R"({"local_ip":"192.168.1.55"})", "application/json");
            statuses[i] = response ? response->status : 0;
        });
        for (auto& racer : racers) racer.join();
        check(std::count(statuses.begin(), statuses.end(), 201) == 1 &&
              std::count(statuses.begin(), statuses.end(), 409) == 7, "concurrent registration");
        if (std::getenv("SPARK_TEST_AUDIT_FAILURE"))
            expect(client.Post("/devices/rollback-probe", R"({"local_ip":"192.168.1.80"})", "application/json"), 503, "database_unavailable");
        // A second HTTP service with a fresh store must see the committed registry.
        httplib::Server restarted;
        spark::configure_api(restarted, std::make_shared<spark::DeviceStore>(config), std::make_shared<spark::TimerStore>(config));
        const int restart_port = restarted.bind_to_any_port("127.0.0.1");
        check(restart_port > 0, "restart listener");
        std::thread restart_worker([&] { restarted.listen_after_bind(); });
        restarted.wait_until_ready();
        try {
            httplib::Client resumed("127.0.0.1", restart_port);
            auto persisted_details = resumed.Get("/devices/watch-1");
            expect(persisted_details, 200);
            check(nlohmann::json::parse(persisted_details->body)["local_ip"] == "192.168.1.50", "GET persisted record");
            expect(resumed.Post("/devices/watch-1", R"({"local_ip":"192.168.1.50"})", "application/json"), 409, "device_exists");
            auto persisted = resumed.Patch("/devices/watch-1", R"({"external_ip":"203.0.113.9"})", "application/json");
            expect(persisted, 200);
            check(nlohmann::json::parse(persisted->body)["local_ip"] == "192.168.1.50", "persistent local IP");
            expect(resumed.Post("/devices/Watch-1", R"({"local_ip":"192.168.1.60"})", "application/json"), 201);
        } catch (...) {
            restarted.stop();
            restart_worker.join();
            throw;
        }
        restarted.stop();
        restart_worker.join();
        // An unavailable database returns a JSON 503 without leaking connector details.
        auto unavailable_config = config;
        unavailable_config.database += "_missing";
        httplib::Server unavailable;
        spark::configure_api(unavailable, std::make_shared<spark::DeviceStore>(unavailable_config), std::make_shared<spark::TimerStore>(unavailable_config));
        const int unavailable_port = unavailable.bind_to_any_port("127.0.0.1");
        check(unavailable_port > 0, "unavailable listener");
        std::thread unavailable_worker([&] { unavailable.listen_after_bind(); });
        unavailable.wait_until_ready();
        try {
            httplib::Client failed("127.0.0.1", unavailable_port);
            expect(failed.Post("/devices/test", R"({"local_ip":"192.168.1.2"})", "application/json"), 503, "database_unavailable");
            expect(failed.Get("/devices/test"), 503, "database_unavailable");
            expect(failed.Get("/health"), 200);
        } catch (...) {
            unavailable.stop();
            unavailable_worker.join();
            throw;
        }
        unavailable.stop();
        unavailable_worker.join();
        std::cout << "API contract checks passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        result = 1;
    }
    server.stop();
    worker.join();
    return result;
}
