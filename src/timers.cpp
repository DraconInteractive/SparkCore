#include "bounded_body.hpp"
#include "timers.hpp"
#include <chrono>
#include <iostream>
#include <regex>

namespace spark {
namespace {
using nlohmann::json;
void reply(httplib::Response& response, int status, const json& body) {
    response.status = status;
    response.set_content(body.dump(2) + "\n", "application/json");
}
bool valid_source(const json& packet) {
    return !packet.contains("source_device") || packet["source_device"].is_null() ||
        (packet["source_device"].is_string() && std::regex_match(packet["source_device"].get<std::string>(), std::regex("[A-Za-z0-9_-]{1,64}")));
}
}
TimerScheduler::TimerScheduler(std::shared_ptr<TimerStore> store) {
    worker_ = std::thread([this, store] {
        bool failed = false;
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stopping_) {
            lock.unlock();
            try {
                store->expire();
                if (failed) std::cerr << "Timer scheduler database connection recovered\n";
                failed = false;
            } catch (const std::exception& error) {
                if (!failed) std::cerr << "Timer scheduler will retry: " << error.what() << '\n';
                failed = true;
            }
            lock.lock();
            wake_.wait_for(lock, std::chrono::seconds(1), [this] { return stopping_; });
        }
    });
}
TimerScheduler::~TimerScheduler() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        stopping_ = true;
    }
    wake_.notify_all();
    worker_.join();
}
void configure_timers(httplib::Server& server, std::shared_ptr<TimerStore> store, std::shared_ptr<Authorization> auth) {
    auto handler = [store](const httplib::Request& request, httplib::Response& response) {
        const bool create = request.path == "/timers" && request.method == "POST";
        const bool list = request.path == "/timers" && (request.method == "GET" || request.method == "HEAD");
        std::string id;
        if (!create && !list) {
            id = request.matches[1].str();
            if (!std::regex_match(id, std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))) {
                reply(response, 400, {{"error", "invalid_timer_id"}});
                return;
            }
        }
        json packet = json::object();
        if (request.method == "POST") {
            if (create || !request.body.empty()) {
                const auto content_type = request.get_header_value("Content-Type");
                if (content_type.substr(0, content_type.find(';')) != "application/json") {
                    reply(response, 415, {{"error", "unsupported_media_type"}});
                    return;
                }
                packet = json::parse(request.body, nullptr, false);
                if (!packet.is_object()) {
                    reply(response, 400, {{"error", "invalid_packet"}});
                    return;
                }
            }
            for (const auto& field : packet.items()) {
                if (field.key() != "source_device" && (!create || (field.key() != "duration_seconds" && field.key() != "label"))) {
                    reply(response, 400, {{"error", "unknown_field"}});
                    return;
                }
            }
            if (!valid_source(packet)) {
                reply(response, 400, {{"error", "invalid_source_device"}});
                return;
            }
            if (create && (!packet.contains("duration_seconds") || !packet["duration_seconds"].is_number_integer() ||
                           packet["duration_seconds"] < 1 || packet["duration_seconds"] > 604800)) {
                reply(response, 400, {{"error", "invalid_duration"}});
                return;
            }
            if (packet.contains("label") && (!packet["label"].is_string() || packet["label"].get<std::string>().size() > 200)) {
                reply(response, 400, {{"error", "invalid_label"}});
                return;
            }
        }
        try {
            if (create) {
                const auto timer = store->create(packet);
                response.set_header("Location", "/timers/" + timer["id"].get<std::string>());
                reply(response, 201, timer);
            } else if (list) {
                reply(response, 200, store->active());
            } else {
                const auto timer = request.method == "POST"
                    ? store->transition(id, request.matches[2].str(), packet.value("source_device", json(nullptr)))
                    : store->get(id);
                if (!timer) reply(response, 404, {{"error", "timer_not_found"}});
                else reply(response, 200, *timer);
            }
        } catch (const TimerConflict&) {
            reply(response, 409, {{"error", "invalid_timer_state"}});
        } catch (const DatabaseError&) {
            reply(response, 503, {{"error", "database_unavailable"}});
        }
    };
    body_route(server,"POST","/timers",handler,auth);
    server.Get("/timers", handler);
    server.Get(R"(/timers/([^/]+))", handler);
    body_route(server,"POST",R"(/timers/([^/]+)/(confirm|stop))",handler,auth);
}
json timer_endpoints() {
    const json timer = {{"id", "12345678-1234-1234-1234-123456789abc"}, {"label", "Tea"},
        {"duration_seconds", 180}, {"status", "running"}, {"source_device", "watch-1"},
        {"created_at", "2026-09-18T00:00:00.000000Z"}, {"ends_at", "2026-09-18T00:03:00.000000Z"},
        {"updated_at", "2026-09-18T00:00:00.000000Z"}, {"remaining_seconds", 180}};
    const json fields = {{"id", "string: server-generated UUID"}, {"label", "string: user label"},
        {"duration_seconds", "integer: original duration"}, {"status", "running, expired, confirmed, or stopped"},
        {"source_device", "string or null: creator ID, metadata only; registration is not required"},
        {"created_at", "UTC ISO 8601 timestamp"}, {"ends_at", "UTC ISO 8601 deadline, retained after stop"},
        {"updated_at", "UTC ISO 8601 last transition timestamp"},
        {"remaining_seconds", "integer: rounded-up remaining duration while running; otherwise zero"}};
    json entries = json::array();
    for (const std::string operation : {"create", "active", "get", "confirm", "stop"}) {
        const bool create = operation == "create";
        const bool list = operation == "active";
        const bool read = list || operation == "get";
        const auto path = create || list ? "/timers" : operation == "get" ? "/timers/{id}" : "/timers/{id}/" + operation;
        json output = timer;
        if (list) output = {{"timers", json::array({timer})}};
        if (operation == "confirm" || operation == "stop") {
            output["status"] = operation == "confirm" ? "confirmed" : "stopped";
            output["remaining_seconds"] = 0;
        }
        entries.push_back({{"method", read ? "GET" : "POST"}, {"path", path},
            {"description", create ? "Start a shared persistent timer. Each POST creates a new timer; retrying can create duplicates." :
                list ? "List running and expired-unconfirmed timers across all devices, ordered by deadline then ID." :
                operation == "get" ? "Get a timer, including confirmed or stopped timers." :
                operation == "confirm" ? "Acknowledge an expired timer. Repeated confirmation is idempotent. Other states return 409." :
                "Stop a running or expired timer. Repeated stop is idempotent. Confirmed timers return 409."},
            {"input", {{"path", create || list ? json::object() : json{{"id", "lowercase UUID from creation response"}}},
                {"query", json::object()}, {"headers", read ? json::object() : json{{"Content-Type", "application/json (required when sending a body)"}}},
                {"body", read ? json(nullptr) : create ? json{{"duration_seconds", 180}, {"label", "Tea"}, {"source_device", "watch-1"}} : json{{"source_device", "phone-1"}}},
                {"fields", read ? json::object() : create ? json{{"duration_seconds", "required integer, 1..604800 seconds"}, {"label", "optional string, at most 200 UTF-8 bytes; default empty"}, {"source_device", "optional device ID or null; metadata only"}} : json{{"source_device", "optional acting device ID or null; empty request body is also accepted"}}},
                {"validation", "Unknown JSON fields rejected. Device IDs: 1-64 ASCII letters, digits, underscores or hyphens."}}},
            {"output", {{"status", create ? 201 : 200}, {"headers", {{"Content-Type", "application/json"}, {"Cache-Control", "no-store"}}},
                {"body", output}, {"fields", list ? json{{"timers", fields}} : fields}}},
            {"errors", {{"400", "invalid_timer_id, invalid_packet, unknown_field, invalid_duration, invalid_label, invalid_source_device"},
                {"404", "timer_not_found"}, {"409", "invalid_timer_state"}, {"415", "unsupported_media_type"}, {"503", "database_unavailable"}}}
        });
    }
    return entries;
}
}
