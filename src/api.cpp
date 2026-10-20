#include "api.hpp"
#include "credential_management.hpp"
#include "browser_auth.hpp"
#include "commands.hpp"
#include "dashboard.hpp"
#include "inter_spark.hpp"
#include "api_docs.hpp"
#include "devices.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include "pulse.hpp"
#include "calendar.hpp"
#include "calendar_sync.hpp"
#include "daily_tasks.hpp"
#include "files.hpp"
#include "folder_downloads.hpp"
#include "projects.hpp"
#include "boards.hpp"
#include <limits>
#include <charconv>
#include <nlohmann/json.hpp>
#include <string>
#include <regex>
#include <vector>

namespace spark {
namespace {
using nlohmann::json;
constexpr auto api_version = "1.42.0";
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

void configure_api(httplib::Server& server, std::shared_ptr<DeviceStore> registry, std::shared_ptr<TimerStore> timers, int notification_port, std::shared_ptr<PulseStore> pulse, std::shared_ptr<CalendarStore> calendar, std::shared_ptr<DailyTaskStore> tasks, std::shared_ptr<FileStorage> files, std::shared_ptr<Maintenance> maintenance, std::shared_ptr<Authorization> auth, const nlohmann::json& public_notifications) {
    if (!files) files = std::make_shared<FileStorage>(std::make_shared<FileStore>(DatabaseConfig::from_environment()), FileStorage::default_roots());
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
          {"api_version", "string: API contract revision"}, {"authentication", "string: none (open mode) or bearer (token mode)"},
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
    if(!public_notifications.is_null()) discovery["notifications"].update(public_notifications);
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
    for(const auto& endpoint:calendar_sync_endpoints())discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : calendar_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : daily_task_endpoints()) discovery["endpoints"].push_back(endpoint);
    discovery["endpoints"].push_back({
        {"method", "GET"}, {"path", "/api/docs"}, {"description", "Human-readable API reference with a searchable endpoint sidebar and request/response details. Requires JavaScript; loads the current /api contract."},
        {"input", {{"headers", json::object()}, {"query", json::object()}, {"body", nullptr}}},
        {"output", {{"status", 200}, {"headers", {{"Content-Type", "text/html; charset=utf-8"}, {"Cache-Control", "no-store"}}}, {"body", "HTML API reference page"}}}
    });
    for (const auto& endpoint : board_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : project_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : folder_download_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : file_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : command_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : backup_visibility_endpoints()) discovery["endpoints"].push_back(endpoint);
    for (const auto& endpoint : dashboard_endpoints()) discovery["endpoints"].push_back(endpoint);
    for(const auto& endpoint:browser_endpoints())discovery["endpoints"].push_back(endpoint);
    for(const auto& endpoint:inter_spark_endpoints())discovery["endpoints"].push_back(endpoint);
    for(const auto& endpoint:credential_endpoints())discovery["endpoints"].push_back(endpoint);
    // Keep the self-example independent of the size of the endpoint catalogue.
    discovery["endpoints"][2]["output"]["body"] = {
        {"name", "SPARK"}, {"server_version", SPARK_VERSION}, {"api_version", api_version},
        {"authentication", "none"}, {"polling", json::object()},
        {"notifications", json::object()}, {"errors", json::array()}, {"endpoints", json::array()}
    };
    discovery["endpoints"][2]["output"]["example_note"] =
        "Abbreviated response shape only. See the top-level polling, notifications, errors and endpoints for their full contents; they are not duplicated here.";
    if(auth) {
        discovery["authentication"]="bearer";
        discovery["endpoints"][2]["output"]["body"]["authentication"]="bearer";
        discovery["notifications"]["authentication"]="hello.token";
        discovery["notifications"]["hello"]["token"]="<device-token>";
        discovery["notifications"]["resume_hello"]["token"]="<device-token>";
        discovery["notifications"]["errors"].push_back("unauthorized");
        discovery["notifications"]["errors"].push_back("forbidden");
        discovery["authorization"]={{"default","deny"},{"credentials","Authorization: Bearer <token>"},{"browser_login","/login; Secure cookie sessions with X-SPARK-CSRF on mutations"},{"revocation","Checked on each request and before TCP output; in-flight HTTP operations/downloads may finish."}};
        discovery["errors"].push_back({{"status",401},{"body",{{"error","unauthorized"}}}});
        discovery["errors"].push_back({{"status",403},{"body",{{"error","forbidden"}}}});
        for(auto& endpoint:discovery["endpoints"]){
            if(endpoint.contains("access"))continue;
            endpoint["input"]["headers"]["Authorization"]="Bearer <token>";
            bool found=false;
            for(const auto& rule:route_policies())if(endpoint["method"]==rule.method&&std::regex_replace(endpoint["path"].get<std::string>(),std::regex(R"(\{[^}]+\})"),"{}") == std::regex_replace(rule.path,std::regex(R"(\{[^}]+\})"),"{}")){endpoint["permissions"]=rule.permissions;found=true;break;}
            // The subscription path uses a descriptive parameter name in discovery.
            if(!found && endpoint["path"]=="/pulse/subscriptions/{device_id}"){endpoint["permissions"]={"pulse.write"};found=true;}
            if(!found)throw std::runtime_error("Endpoint is missing an authorization policy");
        }
    }
    endpoints[2].body = discovery;

    endpoints.push_back({"/api/docs", "Human-readable API reference", nullptr, json::object()});

    server.new_task_queue = [] { return new httplib::ThreadPool(4); };
    server.set_read_timeout(5, 0);
    server.set_write_timeout(5, 0);
    server.set_payload_max_length(std::numeric_limits<std::size_t>::max());
    server.set_default_headers({{"Cache-Control", "no-store"}, {"Access-Control-Allow-Origin", "*"}, {"Access-Control-Expose-Headers", "ETag, Content-Range"}});
    if(auth)server.set_default_headers({{"Cache-Control","no-store"}});
    server.set_pre_routing_handler([endpoints,auth](const auto& request, auto& response) {
        if(auth&&!browser_public_route(request)&&!auth->request(request,response)){
            if(response.status==401&&(request.method=="GET"||request.method=="HEAD")&&!request.has_header("Authorization")&&request.get_header_value("Accept").find("text/html")!=std::string::npos && (request.path=="/"||request.path=="/api/docs"||request.path=="/files/explorer"||request.path=="/projects/explorer"||request.path=="/commands/explorer"||request.path=="/admin/credentials/explorer"||request.path=="/backups/new"||request.path=="/backups/progress"||request.path=="/backups/explorer"||request.path=="/connections/explorer")){
                response.body.clear();response.set_redirect("/login?next="+request.path,303);
            }
            return httplib::Server::HandlerResponse::Handled;
        }
        const bool upload=request.method=="POST" && request.path=="/files";
        const bool text_edit=request.method=="PUT" && std::regex_match(request.path,std::regex(R"(/files/[^/]+/content)"));
        auto reject=[&](int status,const char* error){response.status=status;response.set_header("Connection","close");reply(response,{{"error",error}});return httplib::Server::HandlerResponse::Handled;};
        if(request.has_header("Content-Length")) {
            const auto value=request.get_header_value("Content-Length");std::uint64_t length=0;
            const auto parsed=std::from_chars(value.data(),value.data()+value.size(),length);
            if(parsed.ec!=std::errc{} || parsed.ptr!=value.data()+value.size() || request.get_header_value_count("Content-Length")!=1 || request.has_header("Transfer-Encoding"))return reject(400,"invalid_content_length");
            if(!upload && length>(text_edit?2097152:request.path=="/calendar-sync/apply"?65536:16384))return reject(413,"payload_too_large");
        }
        if(request.has_header("Transfer-Encoding") && request.method!="POST" && request.method!="PATCH" && request.method!="PUT" && request.method!="DELETE")return reject(400,"unexpected_body");
        std::string timer_allow;
        if(request.path=="/commands")timer_allow="GET, HEAD, POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/commands/[^/]+)")))timer_allow="GET, HEAD, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/commands/[^/]+/(renew|complete|cancel))")))timer_allow="POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/devices/[^/]+/actions)")))timer_allow="GET, HEAD, PUT, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/devices/[^/]+/commands/claim)")))timer_allow="POST, OPTIONS";
        if(request.path=="/" || request.path=="/admin/status" || request.path=="/admin/backup-locations") timer_allow="GET, HEAD, OPTIONS";
        if(request.path=="/admin/restart" || request.path=="/admin/backup") {
            if(request.method!="POST") { response.status=405;response.set_header("Allow","POST");return httplib::Server::HandlerResponse::Handled; }
        }
        if(std::regex_match(request.path,std::regex(R"(/project/[^/]+/files)")))timer_allow="GET, HEAD, OPTIONS";
        if(std::regex_match(request.path,std::regex(R"(/projects/[^/]+/boards)")))timer_allow="GET, HEAD, POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/projects/[^/]+/boards/[^/]+)")))timer_allow="GET, HEAD, PATCH, DELETE, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/projects/[^/]+/boards/[^/]+/columns)")))timer_allow="POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/projects/[^/]+/boards/[^/]+/columns/[^/]+)")))timer_allow="PATCH, DELETE, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/projects/[^/]+/boards/[^/]+/columns/[^/]+/tasks)")))timer_allow="POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/projects/[^/]+/boards/[^/]+/tasks/[^/]+)")))timer_allow="GET, HEAD, PATCH, DELETE, OPTIONS";
        if(request.path=="/projects/explorer")timer_allow="GET, HEAD, OPTIONS";
        else if(request.path=="/projects")timer_allow="GET, HEAD, POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/projects/[^/]+)")))timer_allow="GET, HEAD, PATCH, DELETE, OPTIONS";
        if(request.path=="/files/explorer")timer_allow="GET, HEAD, OPTIONS";
        else if(request.path=="/files/downloads")timer_allow="POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/files/downloads/[0-9a-f]{32})")))timer_allow="GET, HEAD, DELETE, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/files/downloads/[0-9a-f]{32}/content)")))timer_allow="GET, HEAD, OPTIONS";
        else if(request.path=="/files/drives")timer_allow="GET, HEAD, POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/files/drives/[A-Z]+)")))timer_allow="DELETE, OPTIONS";
        else if(request.path=="/files/directories")timer_allow="GET, HEAD, POST, PATCH, DELETE, OPTIONS";
        else if(request.path=="/files")timer_allow="GET, HEAD, POST, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/files/[^/]+/content)")))timer_allow="GET, HEAD, PUT, OPTIONS";
        else if(std::regex_match(request.path,std::regex(R"(/files/[^/]+)")))timer_allow="GET, HEAD, PATCH, DELETE, OPTIONS";
        if (request.path == "/daily-tasks") timer_allow = "GET, HEAD, POST, OPTIONS";
        else if (std::regex_match(request.path, std::regex(R"(/daily-tasks/[^/]+)"))) timer_allow = "GET, HEAD, PATCH, DELETE, OPTIONS";
        else if (std::regex_match(request.path, std::regex(R"(/daily-tasks/[^/]+/checked)"))) timer_allow = "PUT, OPTIONS";
        if(request.path=="/calendar-sync"||request.path=="/calendar-sync/changes"||std::regex_match(request.path,std::regex(R"(/calendar-sync/items/[^/]+)")))timer_allow="GET, HEAD, OPTIONS";
        if(request.path=="/calendar-sync/apply")timer_allow="POST, OPTIONS";
        if(std::regex_match(request.path,std::regex(R"(/connections/[0-9a-f]{32}/calendar-sync)")))timer_allow="GET, HEAD, PUT, OPTIONS";
        if(std::regex_match(request.path,std::regex(R"(/connections/[0-9a-f]{32}/calendar-sync/run)")))timer_allow="POST, OPTIONS";
        if(std::regex_match(request.path,std::regex(R"(/connections/[0-9a-f]{32}/calendar-sync/conflicts)")))timer_allow="GET, HEAD, OPTIONS";
        if (request.path == "/events") timer_allow = "GET, HEAD, POST, OPTIONS";
        else if (std::regex_match(request.path,std::regex(R"(/events/[^/]+/move)"))) timer_allow="POST, OPTIONS";
        else if (request.path == "/events/occurrences") timer_allow = "GET, HEAD, OPTIONS";
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
                response.set_header("Access-Control-Allow-Headers", std::regex_match(request.path,std::regex(R"(/files/[^/]+/content)"))?"Content-Type, If-Match, Range":"Content-Type");
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
    configure_browser_auth(server,auth);
    configure_credentials(server,auth);
    server.set_post_routing_handler([](const auto&,auto& response){response.set_header("X-Content-Type-Options","nosniff");response.set_header("X-Frame-Options","DENY");response.set_header("Referrer-Policy","no-referrer");response.set_header("Content-Security-Policy","default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' blob: data:; media-src 'self' blob:; frame-ancestors 'none'; object-src 'none'; base-uri 'none'; form-action 'self'");});
    if(!maintenance)maintenance=std::make_shared<Maintenance>();
    configure_dashboard(server, maintenance, auth);
    configure_backup_visibility(server,maintenance,auth);
    configure_inter_spark(server,files,maintenance,auth,calendar);
    configure_calendar_sync(server,calendar,auth);
    configure_commands(server, std::make_shared<CommandStore>(DatabaseConfig::from_environment()), auth);
    configure_devices(server, std::move(registry), auth);
    configure_timers(server, std::move(timers), auth);
    configure_pulse(server, std::move(pulse), auth);
    configure_calendar(server, std::move(calendar), auth);
    configure_daily_tasks(server, std::move(tasks), auth);
    configure_boards(server, std::make_shared<BoardStore>(DatabaseConfig::from_environment()), files, auth);
    configure_projects(server, std::make_shared<ProjectStore>(DatabaseConfig::from_environment()), files, auth);
    configure_files(server, std::move(files), auth);
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
