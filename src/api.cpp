#include "api.hpp"
#include "api_docs.hpp"
#include "devices.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include "pulse.hpp"
#include "calendar.hpp"
#include "daily_tasks.hpp"
#include "files.hpp"
#include <limits>
#include <charconv>
#include <nlohmann/json.hpp>
#include <string>
#include <regex>
#include <vector>

namespace spark {
namespace {
using nlohmann::json;
constexpr auto api_version = "1.14.0";
struct Endpoint {
    std::string path;
    std::string description;
    json body;
    json fields;
};
void reply(httplib::Response& response, const json& body) {
    response.set_content(body.dump(2) + "\n", "application/json");
}
}

void configure_api(httplib::Server& server, std::shared_ptr<DeviceStore> registry, std::shared_ptr<TimerStore> timers, int notification_port, std::shared_ptr<PulseStore> pulse, std::shared_ptr<CalendarStore> calendar, std::shared_ptr<DailyTaskStore> tasks, std::shared_ptr<FileStorage> files) {
    if (!files) files = std::make_shared<FileStorage>(std::make_shared<FileStore>(DatabaseConfig::from_environment()), FileStorage::default_root());
    if (!tasks) tasks = std::make_shared<DailyTaskStore>(DatabaseConfig::from_environment());
    if (!calendar) calendar = std::make_shared<CalendarStore>(DatabaseConfig::from_environment());
    if (!pulse) pulse = std::make_shared<PulseStore>(DatabaseConfig::from_environment());
    std::vector<Endpoint> endpoints = {
        {"/health", "Liveness check: the SPARK HTTP service is online. Does not check connected devices.",
         {{"status", "ok"}}, {{"status", "string: always ok when the server responds successfully"}}},
        {"/version", "Server release and API contract versions. Compare api_version for contract changes.",
         {{"server_version", SPARK_VERSION}, {"api_version", api_version}},
         {{"server_version", "string: semantic version of the server release"},
          {"api_version", "string: semantic version of the API contract"}}},
        {"/api", "Discover all endpoints, input packets, output packets, and polling conventions.",
         json::object(), {{"name", "string: SPARK"}, {"server_version", "string: server release"},
          {"api_version", "string: API contract revision"}, {"authentication", "string: none"},
          {"polling", "object: polling and compatibility guidance"},
          {"notifications", "object: TCP connection, framing, handshake, heartbeats, and reconnect contract"},
          {"errors", "array: shared HTTP error packets"},
          {"endpoints", "array: method, path, description, input, and output for every endpoint"}}}
    };
    json discovery = {
        {"name", "SPARK"}, {"server_version", SPARK_VERSION}, {"api_version", api_version},
        {"authentication", "none"},
        {"notifications", notification_discovery(notification_port)},
        {"polling", {{"suggested_interval_seconds", 30},
                     {"on_failure", "Use exponential backoff with jitter, capped at 300 seconds."},
                     {"compatibility", "Fetch /api at startup and when /version api_version changes. Major API versions indicate breaking changes; minor versions add compatible features; patch versions clarify or fix the contract. Ignore unknown response fields."}}},
        {"errors", json::array({
            {{"status", 404}, {"body", {{"error", "not_found"}}}},
            {{"status", 405}, {"headers", {{"Allow", "GET, HEAD, OPTIONS"}}}, {"body", {{"error", "method_not_allowed"}}}},
            {{"status", 503}, {"body", {{"error", "database_unavailable"}}}},
            {{"status", 500}, {"body", {{"error", "internal_server_error"}}}}
        })},
        {"endpoints", json::array()}
    };
    for (const auto& endpoint : endpoints) {
        discovery["endpoints"].push_back({
            {"method", "GET"}, {"path", endpoint.path}, {"description", endpoint.description},
            {"input", {{"headers", json::object()}, {"query", json::object()}, {"body", nullptr},
                       {"example", "GET " + endpoint.path + " HTTP/1.1\r\nHost: <server>:47683\r\n\r\n"}}},
            {"output", {{"status", 200}, {"headers", {{"Content-Type", "application/json"}, {"Cache-Control", "no-store"}}},
                        {"fields", endpoint.fields}, {"body", endpoint.body}}}
        });
    }
    for (const auto& endpoint : device_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : timer_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : pulse_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : calendar_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : daily_task_endpoints()) discovery["endpoints"].push_back(endpoint);
    discovery["endpoints"].push_back({
        {"method", "GET"}, {"path", "/api/docs"}, {"description", "Human-readable API reference with a searchable endpoint sidebar and request/response details. Requires JavaScript; loads the current /api contract."},
        {"input", {{"headers", json::object()}, {"query", json::object()}, {"body", nullptr}}},
        {"output", {{"status", 200}, {"headers", {{"Content-Type", "text/html; charset=utf-8"}, {"Cache-Control", "no-store"}}}, {"body", "HTML API reference page"}}}
    });
    for (const auto& endpoint : file_endpoints()) discovery["endpoints"].push_back(endpoint);
    // A finite example avoids recursively embedding the discovery document in itself.
    auto example = discovery;
    example["endpoints"][2]["output"].erase("body");
    example["endpoints"][2]["output"]["example_note"] = "Self-example omitted to terminate recursion.";
    discovery["endpoints"][2]["output"]["body"] = example;
    endpoints[2].body = discovery;

    endpoints.push_back({"/api/docs", "Human-readable API reference", nullptr, json::object()});

    server.new_task_queue = [] { return new httplib::ThreadPool(4); };
    server.set_read_timeout(5, 0);
    server.set_write_timeout(5, 0);
    server.set_payload_max_length(std::numeric_limits<std::size_t>::max());
    server.set_default_headers({{"Cache-Control", "no-store"}, {"Access-Control-Allow-Origin", "*"}});
    server.set_pre_routing_handler([endpoints](const auto& request, auto& response) {
        const bool upload=request.method=="POST" && request.path=="/files";
        auto reject=[&](int status,const char* error){response.status=status;response.set_header("Connection","close");reply(response,{{"error",error}});return httplib::Server::HandlerResponse::Handled;};
        if(request.has_header("Content-Length")) {
            const auto value=request.get_header_value("Content-Length");std::uint64_t length=0;
            const auto parsed=std::from_chars(value.data(),value.data()+value.size(),length);
            if(parsed.ec!=std::errc{} || parsed.ptr!=value.data()+value.size() || request.get_header_value_count("Content-Length")!=1 || request.has_header("Transfer-Encoding"))return reject(400,"invalid_content_length");
            if(!upload && length>16384)return reject(413,"payload_too_large");
        }
        if(request.has_header("Transfer-Encoding") && request.method!="POST" && request.method!="PATCH" && request.method!="PUT" && request.method!="DELETE")return reject(400,"unexpected_body");
        std::string timer_allow;
        if(request.path=="/files/explorer")timer_allow="GET, HEAD, OPTIONS";
        else if(request.path=="/files/directories")timer_allow="GET, HEAD, POST, OPTIONS";
        else if(request.path=="/files")timer_allow="GET, HEAD, POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/files/[^/]+/content)")))timer_allow="GET, HEAD, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/files/[^/]+)")))timer_allow="GET, HEAD, PATCH, DELETE, OPTIONS";
        if (request.path == "/daily-tasks") timer_allow = "GET, HEAD, POST, OPTIONS";
        else if (std::regex_match(request.path, std::regex(R"(/daily-tasks/[^/]+)"))) timer_allow = "GET, HEAD, PATCH, DELETE, OPTIONS";
        else if (std::regex_match(request.path, std::regex(R"(/daily-tasks/[^/]+/checked)"))) timer_allow = "PUT, OPTIONS";
        if (request.path == "/events") timer_allow = "GET, HEAD, POST, OPTIONS";
        else if (std::regex_match(request.path, std::regex(R"(/events/[^/]+)"))) timer_allow = "GET, HEAD, PATCH, DELETE, OPTIONS";
        if (std::regex_match(request.path, std::regex(R"(/pulse/subscriptions/[^/]+)"))) timer_allow = "POST, DELETE, OPTIONS";
        if (request.path == "/pulse") timer_allow = "GET, HEAD, PATCH, OPTIONS";
        if (request.path == "/timers") timer_allow = "GET, HEAD, POST, OPTIONS";
        else if (std::regex_match(request.path, std::regex(R"(/timers/[^/]+)"))) timer_allow = "GET, HEAD, OPTIONS";
        else if (std::regex_match(request.path, std::regex(R"(/timers/[^/]+/(confirm|stop))"))) timer_allow = "POST, OPTIONS";
        if (!timer_allow.empty()) {
            if (request.method == "OPTIONS") {
                response.status = 204;
                response.set_header("Allow", timer_allow);
                response.set_header("Access-Control-Allow-Methods", timer_allow);
                response.set_header("Access-Control-Allow-Headers", "Content-Type");
                return httplib::Server::HandlerResponse::Handled;
            }
            if (timer_allow.find(request.method) == std::string::npos) {
                response.status = 405;
                response.set_header("Allow", timer_allow);
                return httplib::Server::HandlerResponse::Handled;
            }
        }
        if (std::regex_match(request.path, std::regex(R"(/devices/[^/]+)"))) {
            const std::string allow = "GET, HEAD, POST, PATCH, DELETE, OPTIONS";
            if (request.method == "OPTIONS") {
                response.status = 204;
                response.set_header("Allow", allow);
                response.set_header("Access-Control-Allow-Methods", allow);
                response.set_header("Access-Control-Allow-Headers", "Content-Type");
                return httplib::Server::HandlerResponse::Handled;
            }
            if (request.method != "GET" && request.method != "HEAD" && request.method != "POST" && request.method != "PATCH" && request.method != "DELETE") {
                response.status = 405;
                response.set_header("Allow", allow);
                return httplib::Server::HandlerResponse::Handled;
            }
        }
        for (const auto& endpoint : endpoints) {
            if (request.path != endpoint.path) continue;
            if (request.method == "OPTIONS") {
                response.status = 204;
                response.set_header("Allow", "GET, HEAD, OPTIONS");
                response.set_header("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS");
                return httplib::Server::HandlerResponse::Handled;
            }
            if (request.method != "GET" && request.method != "HEAD") {
                response.status = 405;
                response.set_header("Allow", "GET, HEAD, OPTIONS");
                return httplib::Server::HandlerResponse::Handled;
            }
        }
        return httplib::Server::HandlerResponse::Unhandled;
    });
    for (const auto& endpoint : endpoints) {
        if (endpoint.path == "/api/docs") continue;
        server.Get(endpoint.path, [body = endpoint.body](const auto&, auto& response) { reply(response, body); });
    }
    server.Get("/api/docs", [](const auto&, auto& response) {
        response.set_content(std::string(api_docs_html), "text/html; charset=utf-8");
    });
    configure_devices(server, std::move(registry));
    configure_timers(server, std::move(timers));
    configure_pulse(server, std::move(pulse));
    configure_calendar(server, std::move(calendar));
    configure_daily_tasks(server, std::move(tasks));
    configure_files(server, std::move(files));
    // Unknown body routes must not fall back to buffering an unbounded request.
    httplib::Server::HandlerWithContentReader unknown=[](const auto&,auto& response,const auto&){response.status=404;response.set_header("Connection","close");reply(response,{{"error","not_found"}});};
    server.Post(".*",unknown);server.Put(".*",unknown);server.Patch(".*",unknown);server.Delete(".*",unknown);
    server.set_error_handler([](const auto&, auto& response) {
        if (!response.body.empty()) return;
        const auto error = response.status == 404 ? "not_found" :
                           response.status == 405 ? "method_not_allowed" :
                           response.status == 500 ? "internal_server_error" : "http_error";
        reply(response, {{"error", error}});
    });
    server.set_exception_handler([](const auto&, auto& response, std::exception_ptr) {
        response.status = 500;
        reply(response, {{"error", "internal_server_error"}});
    });
}
}
