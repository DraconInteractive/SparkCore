#include "bounded_body.hpp"
#include "calendar.hpp"
#include <charconv>
#include <ctime>
#include <regex>
#include <algorithm>
#include <set>
#include <chrono>
#include <iostream>
namespace spark {
namespace {
using nlohmann::json;
bool date_parts(int year, int month, int day) {
    static const int days[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    if (year < 1000 || year > 9999 || month < 1 || month > 12 || day < 1) return false;
    const bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    return day <= days[month-1] + (month == 2 && leap ? 1 : 0);
}
bool valid_day(const std::string& value) {
    if (!std::regex_match(value, std::regex(R"([0-9]{4}-[0-9]{2}-[0-9]{2})"))) return false;
    return date_parts(std::stoi(value.substr(0,4)), std::stoi(value.substr(5,2)), std::stoi(value.substr(8,2)));
}
std::optional<std::string> timestamp(const json& value) {
    if (!value.is_string()) return std::nullopt;
    const auto input = value.get<std::string>();
    if (!std::regex_match(input, std::regex(R"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2}))")) || !valid_day(input.substr(0,10))) return std::nullopt;
    const int hour = std::stoi(input.substr(11,2)), minute = std::stoi(input.substr(14,2)), second = std::stoi(input.substr(17,2));
    if (hour > 23 || minute > 59 || second > 59) return std::nullopt;
    int offset = 0;
    if (input[19] != 'Z') {
        const int oh = std::stoi(input.substr(20,2)), om = std::stoi(input.substr(23,2));
        if (oh > 14 || om > 59 || (oh == 14 && om != 0)) return std::nullopt;
        offset = (oh * 60 + om) * 60 * (input[19] == '+' ? 1 : -1);
    }
    std::tm parts{};
    parts.tm_year = std::stoi(input.substr(0,4)) - 1900;
    parts.tm_mon = std::stoi(input.substr(5,2)) - 1;
    parts.tm_mday = std::stoi(input.substr(8,2));
    parts.tm_hour = hour; parts.tm_min = minute; parts.tm_sec = second;
    const auto instant = timegm(&parts) - offset;
    std::tm utc{};
    if (!gmtime_r(&instant, &utc) || utc.tm_year < -900 || utc.tm_year > 8099) return std::nullopt;
    char output[32];
    std::strftime(output, sizeof(output), "%Y-%m-%dT%H:%M:%SZ", &utc);
    return std::string(output);
}
bool text_field(const json& value, std::size_t max, bool nonempty = false) {
    if (!value.is_string()) return false;
    const auto text = value.get<std::string>();
    return text.size() <= max && (!nonempty || text.find_first_not_of(" \t\r\n") != std::string::npos);
}
}
std::time_t calendar_instant(const std::string& value) {
    std::tm parts{};
    strptime(value.c_str(), "%Y-%m-%dT%H:%M:%S", &parts);
    return timegm(&parts);
}
std::string calendar_timestamp(std::time_t instant) {
    std::tm parts{}; gmtime_r(&instant, &parts);
    char value[32]; std::strftime(value, sizeof(value), "%Y-%m-%dT%H:%M:%SZ", &parts);
    return value;
}
std::optional<std::time_t> calendar_occurrence(const json& event, std::time_t from) {
    const auto seed = calendar_instant(event["occurs_at"]);
    if (!event.contains("recurrence") || event["recurrence"].is_null())
        return seed >= from ? std::optional<std::time_t>(seed) : std::nullopt;
    const auto& rule = event["recurrence"];
    const auto start = calendar_instant(rule["start_date"].get<std::string>() + "T00:00:00Z");
    const auto end = rule["end_date"].is_null() ? calendar_instant("9999-12-31T00:00:00Z") : calendar_instant(rule["end_date"].get<std::string>() + "T00:00:00Z");
    const auto local_seed = seed + 28800;
    std::tm parts{}; gmtime_r(&local_seed, &parts);
    const int seconds = parts.tm_hour*3600 + parts.tm_min*60 + parts.tm_sec;
    gmtime_r(&start, &parts);
    const auto anchor = start - ((parts.tm_wday+6)%7)*86400;
    auto local = std::max(from, seed) + 28800;
    gmtime_r(&local, &parts); parts.tm_hour=parts.tm_min=parts.tm_sec=0;
    auto day = std::max(start, timegm(&parts));
    const int interval = rule["interval_weeks"];
    // A matching weekday must occur within one recurrence cycle.
    for (int i=0; i<=interval*7+7 && day<=end; ++i, day+=86400) {
        gmtime_r(&day, &parts);
        const int weekday=(parts.tm_wday+6)%7+1;
        const auto candidate=day+seconds-28800;
        if (candidate < from || candidate < seed || ((day-anchor)/604800)%interval != 0) continue;
        for (const auto& selected : rule["weekdays"]) if (selected == weekday) return candidate;
    }
    return std::nullopt;
}
CalendarScheduler::CalendarScheduler(std::shared_ptr<CalendarStore> store) {
    worker_=std::thread([this,store]{
        bool failed=false; std::unique_lock<std::mutex> lock(mutex_);
        while(!stopping_) {
            lock.unlock();
            try { store->tick(); if(failed) std::cerr<<"Calendar scheduler recovered\n"; failed=false; }
            catch(const std::exception& error) { if(!failed) std::cerr<<"Calendar scheduler will retry: "<<error.what()<<'\n'; failed=true; }
            lock.lock(); wake_.wait_for(lock,std::chrono::seconds(1),[this]{return stopping_;});
        }
    });
}
CalendarScheduler::~CalendarScheduler() { {std::lock_guard<std::mutex> lock(mutex_); stopping_=true;} wake_.notify_all(); worker_.join(); }
void configure_calendar(httplib::Server& server, std::shared_ptr<CalendarStore> store) {
    auto handler = [store](const httplib::Request& request, httplib::Response& response) {
        auto reply = [&](int status, const json& body) { response.status = status; response.set_content(body.dump(2) + "\n", "application/json"); };
        auto bad = [&](const char* code) { reply(400, {{"error", code}}); };
        const bool collection = request.path == "/events";
        std::string id;
        if (!collection) {
            id = request.matches[1].str();
            if (!std::regex_match(id, std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))) { bad("invalid_event_id"); return; }
        }
        json packet = json::object(), filters = json::object();
        if (collection && (request.method == "GET" || request.method == "HEAD")) {
            for (const auto& [key, value] : request.params) {
                if (filters.contains(key)) { bad("duplicate_filter"); return; }
                if (key == "from" || key == "to") {
                    auto normalized = timestamp(value);
                    if (!normalized) { bad("invalid_datetime"); return; }
                    filters[key] = *normalized;
                } else if (key == "day") {
                    if (!valid_day(value)) { bad("invalid_day"); return; }
                    filters[key] = value;
                } else if (key == "name" || key == "place") {
                    if (!text_field(value, 200, true)) { bad("invalid_filter"); return; }
                    filters[key] = value;
                } else if (key == "limit" || key == "offset") {
                    int parsed = 0;
                    const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
                    if (result.ec != std::errc{} || result.ptr != value.data() + value.size() || parsed < (key == "limit" ? 1 : 0) || parsed > (key == "limit" ? 500 : 1000000)) { bad("invalid_pagination"); return; }
                    filters[key] = parsed;
                } else { bad("unknown_filter"); return; }
            }
            if (filters.contains("from") && filters.contains("to") && filters["from"].get<std::string>() >= filters["to"].get<std::string>()) { bad("invalid_range"); return; }
        }
        if (request.method == "POST" || request.method == "PATCH") {
            const auto type = request.get_header_value("Content-Type");
            if (type.substr(0, type.find(';')) != "application/json") { reply(415, {{"error", "unsupported_media_type"}}); return; }
            packet = json::parse(request.body, nullptr, false);
            if (!packet.is_object()) { bad("invalid_packet"); return; }
            bool has_field = false;
            for (const auto& field : packet.items()) {
                if (field.key() == "source_device") continue;
                if (field.key() != "name" && field.key() != "description" && field.key() != "occurs_at" && field.key() != "place" && field.key() != "recurrence" && field.key() != "reminder_minutes") { bad("unknown_field"); return; }
                has_field = true;
            }
            if (!has_field || (request.method == "POST" && (!packet.contains("name") || !packet.contains("description") || !packet.contains("occurs_at")))) { bad("missing_fields"); return; }
            if (packet.contains("name") && !text_field(packet["name"], 200, true)) { bad("invalid_name"); return; }
            if (packet.contains("description") && !text_field(packet["description"], 4000)) { bad("invalid_description"); return; }
            if (packet.contains("place") && !packet["place"].is_null() && !text_field(packet["place"], 200, true)) { bad("invalid_place"); return; }
            if (packet.contains("reminder_minutes") && !packet["reminder_minutes"].is_null() &&
                (!packet["reminder_minutes"].is_number_integer() || packet["reminder_minutes"] < 0 || packet["reminder_minutes"] > 10080)) { bad("invalid_reminder_minutes"); return; }
            if (packet.contains("recurrence") && !packet["recurrence"].is_null()) {
                auto& rule=packet["recurrence"];
                if (!rule.is_object() || !rule.contains("weekdays") || !rule.contains("start_date")) { bad("invalid_recurrence"); return; }
                for (const auto& field:rule.items()) if(field.key()!="frequency" && field.key()!="interval_weeks" && field.key()!="weekdays" && field.key()!="start_date" && field.key()!="end_date" && field.key()!="timezone") { bad("invalid_recurrence"); return; }
                if (!rule.contains("frequency")) rule["frequency"]="weekly";
                if (!rule.contains("interval_weeks")) rule["interval_weeks"]=1;
                if (!rule.contains("end_date")) rule["end_date"]=nullptr;
                if (!rule.contains("timezone")) rule["timezone"]="+08:00";
                if (rule["frequency"]!="weekly" || rule["timezone"]!="+08:00" || !rule["interval_weeks"].is_number_integer() || rule["interval_weeks"]<1 || rule["interval_weeks"]>52 ||
                    !rule["start_date"].is_string() || !valid_day(rule["start_date"]) ||
                    (!rule["end_date"].is_null() && (!rule["end_date"].is_string() || !valid_day(rule["end_date"]) || rule["end_date"]<rule["start_date"])) ||
                    !rule["weekdays"].is_array() || rule["weekdays"].empty() || rule["weekdays"].size()>7) { bad("invalid_recurrence"); return; }
                std::set<int> days;
                for (const auto& day:rule["weekdays"]) {
                    if (!day.is_number_integer() || day<1 || day>7 || !days.insert(day.get<int>()).second) { bad("invalid_recurrence"); return; }
                }
                rule["weekdays"]=days;
            }
            if (packet.contains("occurs_at")) {
                auto normalized = timestamp(packet["occurs_at"]);
                if (!normalized) { bad("invalid_datetime"); return; }
                packet["occurs_at"] = *normalized;
            }
            if (packet.contains("source_device") && !packet["source_device"].is_null() &&
                (!packet["source_device"].is_string() || !std::regex_match(packet["source_device"].get<std::string>(), std::regex("[A-Za-z0-9_-]{1,64}")))) { bad("invalid_source_device"); return; }
        }
        try {
            if (request.method == "POST") {
                const auto event = store->create(packet);
                response.set_header("Location", "/events/" + event["id"].get<std::string>());
                reply(201, event);
            } else if (request.method == "DELETE") {
                if (!store->remove(id)) reply(404, {{"error", "event_not_found"}});
                else response.status = 204;
            } else if (collection) reply(200, store->list(filters));
            else {
                const auto event = request.method == "PATCH" ? store->patch(id, packet) : store->get(id);
                if (!event) reply(404, {{"error", "event_not_found"}}); else reply(200, *event);
            }
        } catch (const std::invalid_argument& error) { bad(error.what()); } catch (const DatabaseError&) { reply(503, {{"error", "database_unavailable"}}); }
    };
    body_route(server,"POST","/events", handler); server.Get("/events", handler);
    server.Get(R"(/events/([^/]+))", handler); body_route(server,"PATCH",R"(/events/([^/]+))", handler); body_route(server,"DELETE",R"(/events/([^/]+))", handler);
}
json calendar_endpoints() {
    const json event = {{"id", "12345678-1234-1234-1234-123456789abc"}, {"name", "Walk"}, {"description", "Meet at the entrance"},
        {"occurs_at", "2026-09-19T01:00:00Z"}, {"place", "Park"}, {"recurrence", nullptr}, {"reminder_minutes", nullptr}, {"last_reminder_occurs_at", nullptr}, {"created_at", "2026-09-18T00:00:00.000000Z"}, {"updated_at", "2026-09-18T00:00:00.000000Z"}};
    json endpoints = json::array();
    for (const std::string operation : {"create", "list", "get", "patch", "delete"}) {
        const bool list = operation == "list", remove = operation == "delete", create = operation == "create", write = create || operation == "patch";
        const json input_fields = {{"name", "nonblank string, max 200 UTF-8 bytes"}, {"description", "string, max 4000 UTF-8 bytes; may be empty"},
            {"occurs_at", "YYYY-MM-DDTHH:MM:SSZ or explicit +/-HH:MM offset (max 14:00); normalized to UTC; years 1000..9999"},
            {"recurrence", "null (one-off) or weekly rule: frequency=weekly, interval_weeks=1..52 (default 1), weekdays=distinct ISO weekdays 1 Monday..7 Sunday, start_date=YYYY-MM-DD, end_date=inclusive YYYY-MM-DD or null, timezone=+08:00. Monday-based weeks anchored to start_date. occurs_at sets Perth clock time and earliest occurrence. PATCH replaces whole rule; null clears it."},
            {"reminder_minutes", "optional integer 0..10080 before each occurrence; null disables. TCP calendar.reminder includes occurrence_at. At most once per occurrence; schedule changes reset delivery state."},
            {"place", "optional nonblank string max 200 UTF-8 bytes or null"}, {"source_device", "optional device ID or null for mutation attribution"}};
        endpoints.push_back({{"method", create ? "POST" : remove ? "DELETE" : operation == "patch" ? "PATCH" : "GET"},
            {"path", create || list ? "/events" : "/events/{id}"},
            {"description", list ? "List event series with at least one occurrence matching the date filters (each series once). Filters combine with AND; sorted by occurs_at then ID. Follow pagination for all results." :
                create ? "Create a dated event; name, description and occurs_at required. Each POST creates a new UUID." :
                remove ? "Delete a dated event. Missing IDs return 404; success has no body." :
                operation == "patch" ? "Partially modify an event. Omitted fields retained; place:null clears the place. Identical updates do not emit notifications." : "Retrieve a dated event by UUID."},
            {"input", {{"path", create || list ? json::object() : json{{"id", "lowercase UUID from creation response"}}},
                {"headers", write ? json{{"Content-Type", "application/json"}} : json::object()},
                {"query", list ? json{{"place", "exact case-sensitive place"}, {"name", "literal case-sensitive name substring"}, {"day", "UTC calendar date YYYY-MM-DD"},
                    {"from", "inclusive timestamp, explicit zone required"}, {"to", "exclusive timestamp, explicit zone required"}, {"limit", "1..500; default 100"}, {"offset", "0..1000000; default 0"}} : json::object()},
                {"fields", write ? input_fields : json::object()}, {"body", write ? json{{"name", "Walk"}, {"description", "Meet at the entrance"}, {"occurs_at", "2026-09-19T09:00:00+08:00"}, {"place", "Park"}} : json(nullptr)},
                {"validation", "Unknown fields/filters and duplicate filters rejected. PATCH needs at least one editable field. Query '+' offsets must be URL encoded as %2B."}}},
            {"output", {{"status", create ? 201 : remove ? 204 : 200}, {"headers", remove ? json{{"Cache-Control", "no-store"}} : json{{"Content-Type", "application/json"}, {"Cache-Control", "no-store"}}},
                {"fields", remove ? json::object() : list ? json{{"events", "array of event records"}, {"limit", "page size"}, {"offset", "page offset"}, {"has_more", "boolean: another page exists"}} : json{{"id", "UUID"}, {"name", "string"}, {"description", "string"}, {"occurs_at", "UTC ISO 8601 timestamp"}, {"place", "string or null"}, {"recurrence", "weekly rule or null"}, {"reminder_minutes", "integer or null"}, {"last_reminder_occurs_at", "last reminded occurrence UTC timestamp or null"}, {"created_at", "UTC creation timestamp"}, {"updated_at", "UTC last-change timestamp"}}},
                {"body", remove ? json(nullptr) : list ? json{{"events", json::array({event})}, {"limit", 100}, {"offset", 0}, {"has_more", false}} : event}}},
            {"errors", {{"400", "invalid_event_id, invalid_packet, missing_fields, unknown_field, invalid_name, invalid_description, invalid_place, invalid_datetime, invalid_source_device, invalid_recurrence, invalid_reminder_minutes, unknown_filter, duplicate_filter, invalid_filter, invalid_day, invalid_range, invalid_pagination"}, {"404", "event_not_found"}, {"415", "unsupported_media_type"}, {"503", "database_unavailable"}}}
        });
    }
    return endpoints;
}
}
