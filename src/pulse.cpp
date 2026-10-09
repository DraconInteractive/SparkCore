#include "bounded_body.hpp"
#include "pulse.hpp"
#include <chrono>
#include <iostream>
#include <regex>
namespace spark {
using nlohmann::json;
PulseScheduler::PulseScheduler(std::shared_ptr<PulseStore> store) {
    worker_ = std::thread([this, store] {
        bool failed = false;
        std::unique_lock<std::mutex> lock(mutex_);
        while (!stopping_) {
            lock.unlock();
            try {
                store->tick();
                if (failed) std::cerr << "Pulse scheduler recovered\n";
                failed = false;
            } catch (const std::exception& error) {
                if (!failed) std::cerr << "Pulse scheduler will retry: " << error.what() << '\n';
                failed = true;
            }
            lock.lock();
            wake_.wait_for(lock, std::chrono::seconds(1), [this] { return stopping_; });
        }
    });
}
PulseScheduler::~PulseScheduler() {
    { std::lock_guard<std::mutex> lock(mutex_); stopping_ = true; }
    wake_.notify_all();
    worker_.join();
}
void configure_pulse(httplib::Server& server, std::shared_ptr<PulseStore> store, std::shared_ptr<Authorization> auth) {
    auto handler = [store](const httplib::Request& request, httplib::Response& response) {
        auto reply = [&](int status, const json& body) {
            response.status = status;
            response.set_content(body.dump(2) + "\n", "application/json");
        };
        json packet;
        if (request.method == "PATCH") {
            const auto type = request.get_header_value("Content-Type");
            if (type.substr(0, type.find(';')) != "application/json") { reply(415, {{"error", "unsupported_media_type"}}); return; }
            packet = json::parse(request.body, nullptr, false);
            if (!packet.is_object()) { reply(400, {{"error", "invalid_packet"}}); return; }
            for (const auto& field : packet.items())
                if (field.key() != "enabled" && field.key() != "interval_seconds" && field.key() != "source_device") {
                    reply(400, {{"error", "unknown_field"}}); return;
                }
            if (!packet.contains("enabled") && !packet.contains("interval_seconds")) { reply(400, {{"error", "missing_fields"}}); return; }
            if (packet.contains("enabled") && !packet["enabled"].is_boolean()) { reply(400, {{"error", "invalid_enabled"}}); return; }
            if (packet.contains("interval_seconds") && (!packet["interval_seconds"].is_number_integer() || packet["interval_seconds"] < 1 || packet["interval_seconds"] > 604800)) {
                reply(400, {{"error", "invalid_interval"}}); return;
            }
            if (packet.contains("source_device") && !packet["source_device"].is_null() &&
                (!packet["source_device"].is_string() || !std::regex_match(packet["source_device"].get<std::string>(), std::regex("[A-Za-z0-9_-]{1,64}")))) {
                reply(400, {{"error", "invalid_source_device"}}); return;
            }
        }
        try { reply(200, request.method == "PATCH" ? store->patch(packet) : store->get()); }
        catch (const DatabaseError&) { reply(503, {{"error", "database_unavailable"}}); }
    };
    auto subscription = [store](const httplib::Request& request, httplib::Response& response) {
        const auto id = request.matches[1].str();
        auto reply = [&](int status, const json& body) {
            response.status = status;
            response.set_content(body.dump(2) + "\n", "application/json");
        };
        if (!std::regex_match(id, std::regex("[A-Za-z0-9_-]{1,64}"))) {
            reply(400, {{"error", "invalid_id"}}); return;
        }
        if (!request.body.empty()) { reply(400, {{"error", "unexpected_body"}}); return; }
        try {
            const bool subscribed = request.method == "POST";
            if (subscribed) store->subscribe(id); else store->unsubscribe(id);
            reply(200, {{"device_id", id}, {"subscribed", subscribed}});
        } catch (const DatabaseError&) { reply(503, {{"error", "database_unavailable"}}); }
    };
    body_route(server,"POST",R"(/pulse/subscriptions/([^/]+))", subscription,auth);
    body_route(server,"DELETE",R"(/pulse/subscriptions/([^/]+))", subscription,auth);
    server.Get("/pulse", handler);
    body_route(server,"PATCH","/pulse",handler,auth);
}
json pulse_endpoints() {
    json endpoints = json::array();
    for (const std::string method : {"GET", "PATCH"}) {
        const bool read = method == "GET";
        endpoints.push_back({{"method", method}, {"path", "/pulse"},
            {"description", read ? "Read the shared pulse schedule and most recent pulse; does not fire a pulse." :
             "Enable/disable or change pulse interval. Changed settings restart the countdown; identical settings are idempotent. Emits pulse.updated, not a reminder."},
            {"input", {{"headers", read ? json::object() : json{{"Content-Type", "application/json"}}}, {"query", json::object()},
                {"body", read ? json(nullptr) : json{{"enabled", true}, {"interval_seconds", 3600}}},
                {"fields", read ? json::object() : json{{"enabled", "optional boolean"}, {"interval_seconds", "optional integer 1..604800"}, {"source_device", "optional device ID or null for attribution"}}},
                {"validation", "PATCH requires enabled and/or interval_seconds; unknown fields rejected. No authentication."}}},
            {"output", {{"status", 200}, {"headers", {{"Content-Type", "application/json"}, {"Cache-Control", "no-store"}}},
                {"fields", {{"enabled", "boolean"}, {"interval_seconds", "integer"}, {"next_pulse_at", "UTC ISO 8601 timestamp or null while disabled"},
                    {"last_pulse_at", "UTC ISO 8601 timestamp or null before first pulse"}, {"pulse_count", "monotonically increasing shared pulse number"}, {"updated_at", "UTC ISO 8601 timestamp"}}},
                {"body", {{"enabled", false}, {"interval_seconds", 3600}, {"next_pulse_at", nullptr}, {"last_pulse_at", nullptr}, {"pulse_count", 0}, {"updated_at", "2026-09-18T00:00:00.000000Z"}}}}},
            {"errors", {{"400", "invalid_packet, unknown_field, missing_fields, invalid_enabled, invalid_interval, invalid_source_device"}, {"415", "unsupported_media_type"}, {"503", "database_unavailable"}}}
        });
    }
    for (const std::string method : {"POST", "DELETE"}) {
        endpoints.push_back({{"method", method}, {"path", "/pulse/subscriptions/{device_id}"},
            {"description", method == "POST" ? "Persistently subscribe a TCP hello device ID to pulse events. Repeating is a no-op." : "Unsubscribe a device from future pulse dispatches. Repeating succeeds; already queued notifications may still arrive."},
            {"input", {{"path", {{"device_id", "1-64 ASCII letters, digits, underscores or hyphens; case-sensitive; must match TCP hello ID"}}},
                {"headers", json::object()}, {"query", json::object()}, {"body", nullptr}, {"validation", "No body. Registration not required; no authentication."}}},
            {"output", {{"status", 200}, {"headers", {{"Content-Type", "application/json"}, {"Cache-Control", "no-store"}}},
                {"fields", {{"device_id", "string: subscription identity"}, {"subscribed", "boolean: resulting subscription state"}}},
                {"body", {{"device_id", "watch-1"}, {"subscribed", method == "POST"}}}}},
            {"errors", {{"400", "invalid_id or unexpected_body"}, {"503", "database_unavailable"}}}
        });
    }
    return endpoints;
}
}
