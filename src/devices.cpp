#include "bounded_body.hpp"
#include "devices.hpp"
#include "database.hpp"
#include <array>
#include <memory>
#include <regex>

namespace spark {
namespace {
using nlohmann::json;
void respond(httplib::Response& response, int status, const json& body) {
    response.status = status;
    response.set_content(body.dump(2) + "\n", "application/json");
}
bool valid_ip(const std::string& value) {
    std::array<unsigned char, 16> address{};
    return inet_pton(AF_INET, value.c_str(), address.data()) == 1 ||
           inet_pton(AF_INET6, value.c_str(), address.data()) == 1;
}
bool public_ip(const std::string& value) {
    std::array<unsigned char, 16> b{};
    if (inet_pton(AF_INET, value.c_str(), b.data()) == 1) {
        return !(b[0] == 0 || b[0] == 10 || b[0] == 127 || b[0] >= 224 ||
            (b[0] == 100 && b[1] >= 64 && b[1] <= 127) ||
            (b[0] == 169 && b[1] == 254) || (b[0] == 172 && b[1] >= 16 && b[1] <= 31) ||
            (b[0] == 192 && (b[1] == 168 || b[1] == 0 || (b[1] == 88 && b[2] == 99))) ||
            (b[0] == 198 && (b[1] == 18 || b[1] == 19 || (b[1] == 51 && b[2] == 100))) ||
            (b[0] == 203 && b[1] == 0 && b[2] == 113));
    }
    if (inet_pton(AF_INET6, value.c_str(), b.data()) != 1) return false;
    // Only infer from ordinary global unicast; exclude special-purpose blocks.
    return (b[0] & 0xe0) == 0x20 &&
        !(b[0] == 0x20 && b[1] == 0x01 && (b[2] < 2 || (b[2] == 0x0d && b[3] == 0xb8))) &&
        !(b[0] == 0x20 && b[1] == 0x02) && !(b[0] == 0x3f && b[1] == 0xff);
}
}
void configure_devices(httplib::Server& server, std::shared_ptr<DeviceStore> registry, std::shared_ptr<Authorization> auth) {
    auto handler = [registry](const httplib::Request& request, httplib::Response& response) {
        const auto id = request.matches[1].str();
        if (!std::regex_match(id, std::regex("[A-Za-z0-9_-]{1,64}"))) {
            respond(response, 400, {{"error", "invalid_id"}});
            return;
        }
        json packet;
        if (request.method == "POST" || request.method == "PATCH") {
            const auto type = request.get_header_value("Content-Type");
            if (type.substr(0, type.find(';')) != "application/json") {
                respond(response, 415, {{"error", "unsupported_media_type"}});
                return;
            }
            packet = json::parse(request.body, nullptr, false);
            if (!packet.is_object()) {
                respond(response, 400, {{"error", "invalid_packet"}});
                return;
            }
            for (const auto& field : packet.items()) {
                if (field.key() != "id" && field.key() != "local_ip" && field.key() != "external_ip") {
                    respond(response, 400, {{"error", "unknown_field"}});
                    return;
                }
            }
            if (packet.contains("id") && packet["id"] != id) {
                respond(response, 400, {{"error", "id_mismatch"}});
                return;
            }
            if ((request.method == "POST" && !packet.contains("local_ip")) ||
                (request.method == "PATCH" && !packet.contains("local_ip") && !packet.contains("external_ip"))) {
                respond(response, 400, {{"error", "missing_fields"}});
                return;
            }
            for (const auto* field : {"local_ip", "external_ip"}) {
                if (!packet.contains(field) || (std::string(field) == "external_ip" && packet[field].is_null())) continue;
                if (!packet[field].is_string() || !valid_ip(packet[field].get<std::string>()) ||
                    packet[field].get<std::string>().find('\0') != std::string::npos) {
                    respond(response, 400, {{"error", "invalid_ip"}, {"field", field}});
                    return;
                }
            }
        }
        try {
            if (request.method == "GET" || request.method == "HEAD") {
                auto device = registry->get(id);
                if (!device) respond(response, 404, {{"error", "device_not_found"}});
                else respond(response, 200, *device);
            } else if (request.method == "POST") {
                json device = {{"id", id}, {"local_ip", packet["local_ip"]}, {"external_ip", nullptr}};
                if (packet.contains("external_ip")) device["external_ip"] = packet["external_ip"];
                else if (public_ip(request.remote_addr)) device["external_ip"] = request.remote_addr;
                if (!registry->create(device)) respond(response, 409, {{"error", "device_exists"}});
                else respond(response, 201, device);
            } else if (request.method == "DELETE") {
                if (!registry->remove(id)) respond(response, 404, {{"error", "device_not_found"}});
                else response.status = 204;
            } else {
                auto device = registry->patch(id, packet);
                if (!device) respond(response, 404, {{"error", "device_not_found"}});
                else respond(response, 200, *device);
            }
        } catch (const DatabaseError&) {
            respond(response, 503, {{"error", "database_unavailable"}});
        }
    };
    server.Get(R"(/devices/([^/]+))", handler);
    body_route(server,"POST",R"(/devices/([^/]+))",handler,auth);
    body_route(server,"PATCH",R"(/devices/([^/]+))",handler,auth);
    body_route(server,"DELETE",R"(/devices/([^/]+))",handler,auth);
}

json device_endpoints() {
    json entries = json::array();
    const json device = {{"id", "watch-1"}, {"local_ip", "192.168.1.50"}, {"external_ip", nullptr}};
    for (const std::string method : {"GET", "POST", "PATCH", "DELETE"}) {
        const bool remove = method == "DELETE";
        const bool no_body = remove || method == "GET";
        json body = no_body ? json(nullptr) : json{{"local_ip", "192.168.1.50"}};
        json errors = {{"400", "invalid_id"}, {"404", "device_not_found"}, {"503", "database_unavailable"}};
        if (!no_body) {
            errors["400"] = "invalid_id, invalid_packet, unknown_field, id_mismatch, missing_fields, or invalid_ip (includes field)";
            errors["415"] = "unsupported_media_type";
        }
        if (method == "POST") errors["409"] = "device_exists";
        auto output_device = device;
        if (method == "GET") output_device["subscribed"] = false;
        entries.push_back({
            {"method", method}, {"path", "/devices/{id}"},
            {"description", method == "GET" ? "Fetch the current device record and pulse subscription state from MariaDB. Missing IDs return 404 with device_not_found." : method == "POST" ? "Register a device in MariaDB; records persist across restarts. Duplicate IDs return 409." :
                method == "PATCH" ? "Update an existing device. Omitted fields are preserved; external_ip:null clears the external address." :
                "Remove a device. Missing IDs return 404; success has no response body."},
            {"input", {{"path", {{"id", "Required: 1-64 ASCII letters, digits, underscores or hyphens; case-sensitive."}}},
                {"headers", no_body ? json::object() : json{{"Content-Type", "application/json"}}},
                {"query", json::object()}, {"body", body},
                {"fields", no_body ? json::object() : json{
                    {"id", "Optional; must equal the URL id. IDs cannot be changed."},
                    {"local_ip", "IPv4 or IPv6 literal. Required on POST; optional on PATCH; cannot be null."},
                    {"external_ip", "Optional IPv4/IPv6 literal or null. POST omission infers from a public connection source, otherwise null. Forwarded headers are ignored. PATCH omission preserves the value."}}},
                {"validation", no_body ? "No request body." : "Unknown fields are rejected. PATCH requires at least one IP field."}}},
            {"output", {{"status", remove ? 204 : method == "POST" ? 201 : 200},
                {"headers", remove ? json{{"Cache-Control", "no-store"}} : json{{"Content-Type", "application/json"}, {"Cache-Control", "no-store"}}},
                {"fields", remove ? json::object() : json{{"id", "string: device identifier"}, {"local_ip", "string: local IP"}, {"external_ip", "string or null: external IP if known"}}},
                {"body", remove ? json(nullptr) : output_device}}},
            {"errors", errors}
        });
        if (method == "GET") entries.back()["output"]["fields"]["subscribed"] = "boolean: whether this device ID is subscribed to pulse notifications";
    }
    return entries;
}
}
