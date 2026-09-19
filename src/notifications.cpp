#include "notifications.hpp"
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <regex>
#include <thread>
#include <vector>

namespace spark {
using nlohmann::json;
namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t max_clients = 128, max_frame = 4096, max_output = 262144;
int listener(const NotificationOptions& options, int& port) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    addrinfo* raw = nullptr;
    const auto service = std::to_string(options.port);
    if (getaddrinfo(options.host.c_str(), service.c_str(), &hints, &raw))
        throw std::runtime_error("Cannot resolve notification bind address");
    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> addresses(raw, freeaddrinfo);
    int fd = -1;
    for (auto address = raw; address; address = address->ai_next) {
        fd = socket(address->ai_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) continue;
        int reuse = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        if (bind(fd, address->ai_addr, address->ai_addrlen) == 0 && listen(fd, 32) == 0) break;
        close(fd);
        fd = -1;
    }
    if (fd < 0) throw std::runtime_error("Cannot bind TCP notification listener");
    sockaddr_storage bound{};
    socklen_t length = sizeof(bound);
    if (getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &length) != 0) {
        close(fd);
        throw std::runtime_error("Cannot inspect notification listener");
    }
    port = bound.ss_family == AF_INET ? ntohs(reinterpret_cast<sockaddr_in*>(&bound)->sin_port)
                                      : ntohs(reinterpret_cast<sockaddr_in6*>(&bound)->sin6_port);
    return fd;
}
}
struct NotificationServer::Impl {
    struct Client {
        int fd;
        bool ready = false, closing = false;
        std::string input, output, device_id;
        Clock::time_point connected = Clock::now(), received = connected, pinged = connected;
    };
    std::shared_ptr<NotificationStore> store;
    NotificationOptions options;
    int socket_fd = -1, bound_port = 0;
    std::atomic<bool> stopping{false};
    std::thread publisher, network;
    std::mutex mutex;
    std::condition_variable wake;
    struct Frame {
        std::string text;
        std::shared_ptr<const std::unordered_set<std::string>> recipients;
    };
    std::deque<Frame> queue;
    bool reset = false;
    unsigned long long cursor = 0;
    explicit Impl(std::shared_ptr<NotificationStore> s, NotificationOptions o) : store(std::move(s)), options(std::move(o)) {
        store->verify();
        // Connected clients always resync via REST, so historical replay isn't required.
        cursor = store->latest();
        socket_fd = listener(options, bound_port);
        try {
            publisher = std::thread([this] { publish(); });
            network = std::thread([this] { serve(); });
        } catch (...) {
            stopping = true;
            wake.notify_all();
            if (publisher.joinable()) publisher.join();
            close(socket_fd);
            throw;
        }
    }
    ~Impl() {
        stopping = true;
        wake.notify_all();
        if (network.joinable()) network.join();
        if (publisher.joinable()) publisher.join();
        close(socket_fd);
    }
    void publish() {
        bool failed = false;
        while (!stopping) {
            try {
                store->sequence_events();
                std::shared_ptr<const std::unordered_set<std::string>> subscribers;
                for (const auto& event : store->after(cursor)) {
                    const bool pulse = event["type"].get<std::string>().rfind("pulse.", 0) == 0;
                    if (pulse && !subscribers) subscribers = std::make_shared<const std::unordered_set<std::string>>(store->pulse_subscribers());
                    const auto frame = event.dump() + "\n";
                    std::lock_guard<std::mutex> lock(mutex);
                    if (queue.size() >= 2048) { queue.clear(); reset = true; }
                    queue.push_back({frame, pulse ? subscribers : nullptr});
                    cursor = event["sequence"].get<unsigned long long>();
                }
                if (failed) std::cerr << "Notification event polling recovered\n";
                failed = false;
            } catch (const std::exception& error) {
                if (!failed) std::cerr << "Notification event polling will retry: " << error.what() << '\n';
                // Force clients to reconnect/resync after a database outage.
                if (!failed) { std::lock_guard<std::mutex> lock(mutex); reset = true; }
                failed = true;
            }
            std::unique_lock<std::mutex> lock(mutex);
            wake.wait_for(lock, std::chrono::milliseconds(250), [this] { return stopping.load(); });
        }
    }
    static void error(Client& client, const char* code) {
        client.output = json{{"type", "error"}, {"error", code}}.dump() + "\n";
        client.closing = true;
    }
    void message(Client& client, const std::string& line) {
        const auto packet = json::parse(line, nullptr, false);
        if (!packet.is_object() || !packet.contains("type") || !packet["type"].is_string()) {
            error(client, "invalid_message"); return;
        }
        const std::string type = packet["type"];
        if (!client.ready) {
            if (type != "hello" || !packet.contains("device_id") || !packet["device_id"].is_string() ||
                !std::regex_match(packet["device_id"].get<std::string>(), std::regex("[A-Za-z0-9_-]{1,64}")) || packet.size() != 2) {
                error(client, "invalid_hello"); return;
            }
            client.device_id = packet["device_id"].get<std::string>();
            client.ready = true;
            client.output += json{{"type", "welcome"}, {"protocol_version", 1}, {"device_id", packet["device_id"]},
                {"resync_required", true}, {"resources", json::array({"/timers", "/pulse", "/events", "/daily-tasks"})},
                {"heartbeat_interval_seconds", options.heartbeat.count() / 1000},
                {"idle_timeout_seconds", options.idle_timeout.count() / 1000}}.dump() + "\n";
        } else if (type == "ping" && packet.size() == 1) client.output += "{\"type\":\"pong\"}\n";
        else if (type != "pong" || packet.size() != 1) error(client, "unsupported_message");
        client.received = Clock::now();
    }
    void serve() {
        std::vector<Client> clients;
        while (!stopping) {
            std::deque<Frame> events;
            bool disconnect = false;
            {
                std::lock_guard<std::mutex> lock(mutex);
                events.swap(queue);
                disconnect = reset;
                reset = false;
            }
            if (disconnect) {
                for (auto& client : clients) close(client.fd);
                clients.clear();
            }
            for (auto& client : clients) if (client.ready && !client.closing) {
                for (const auto& frame : events) {
                    if (frame.recipients && !frame.recipients->count(client.device_id)) continue;
                    if (client.output.size() + frame.text.size() > max_output) { client.closing = true; client.output.clear(); break; }
                    client.output += frame.text;
                }
            }
            std::vector<pollfd> fds{{socket_fd, POLLIN, 0}};
            for (const auto& client : clients)
                fds.push_back({client.fd, static_cast<short>((client.closing ? 0 : POLLIN) | (client.output.empty() ? 0 : POLLOUT)), 0});
            const int result = poll(fds.data(), fds.size(), 100);
            if (result < 0) { if (errno == EINTR) continue; break; }
            const auto now = Clock::now();
            for (std::size_t i = clients.size(); i-- > 0;) {
                auto& client = clients[i];
                const auto flags = fds[i + 1].revents;
                bool dead = flags & (POLLERR | POLLHUP | POLLNVAL);
                if (!dead && (flags & POLLIN)) {
                    char buffer[4096];
                    const auto count = recv(client.fd, buffer, sizeof(buffer), 0);
                    if (count == 0) dead = true;
                    else if (count < 0) { if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) dead = true; }
                    else {
                        client.input.append(buffer, count);
                        std::size_t newline;
                        while (!client.closing && (newline = client.input.find('\n')) != std::string::npos) {
                            if (newline > max_frame) { error(client, "frame_too_large"); break; }
                            auto line = client.input.substr(0, newline);
                            client.input.erase(0, newline + 1);
                            if (!line.empty() && line.back() == '\r') line.pop_back();
                            message(client, line);
                            if (client.output.size() > max_output) { dead = true; break; }
                        }
                        if (client.input.size() > max_frame) error(client, "frame_too_large");
                    }
                }
                if (!dead && !client.output.empty() && (flags & POLLOUT)) {
                    const auto count = send(client.fd, client.output.data(), client.output.size(), MSG_NOSIGNAL);
                    if (count > 0) client.output.erase(0, count);
                    else if (count < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) dead = true;
                }
                if ((!client.ready && now - client.connected > options.hello_timeout) ||
                    now - client.received > options.idle_timeout || (client.closing && client.output.empty())) dead = true;
                if (client.ready && !client.closing && now - client.pinged >= options.heartbeat) {
                    if (client.output.size() + 16 > max_output) dead = true;
                    else client.output += "{\"type\":\"ping\"}\n";
                    client.pinged = now;
                }
                if (dead) { close(client.fd); clients.erase(clients.begin() + i); }
            }
            if (fds[0].revents & POLLIN) {
                // Bound accepts per loop so bursts cannot starve existing clients.
                for (int i = 0; i < 16; ++i) {
                    const int fd = accept4(socket_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (fd < 0) break;
                    if (clients.size() >= max_clients) close(fd);
                    else { int keepalive = 1; setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)); clients.push_back(Client{fd}); }
                }
            }
        }
        for (auto& client : clients) close(client.fd);
    }
};
NotificationServer::NotificationServer(std::shared_ptr<NotificationStore> store, NotificationOptions options)
    : impl_(std::make_unique<Impl>(std::move(store), std::move(options))) {}
NotificationServer::~NotificationServer() = default;
int NotificationServer::port() const { return impl_->bound_port; }
json notification_discovery(int port) {
    return {{"transport", "tcp"}, {"port", port}, {"protocol_version", 1}, {"encoding", "UTF-8 newline-delimited JSON"},
        {"authentication", "none"}, {"hello", {{"type", "hello"}, {"device_id", "watch-1"}}},
        {"welcome", {{"type", "welcome"}, {"protocol_version", 1}, {"device_id", "watch-1"}, {"resync_required", true},
                     {"resources", json::array({"/timers", "/pulse", "/events", "/daily-tasks"})}, {"heartbeat_interval_seconds", 30}, {"idle_timeout_seconds", 90}}},
        {"notification", {{"sequence", 42}, {"event_id", 123}, {"type", "timer.created"}, {"resource", "/timers/<id>"}, {"source_device", "watch-1"}}},
        {"event_types", json::array({"timer.created", "timer.expired", "timer.confirmed", "timer.stopped", "pulse.updated", "pulse.triggered", "calendar.created", "calendar.updated", "calendar.deleted", "calendar.reminder", "daily_tasks.updated", "daily_tasks.reset"})},
        {"heartbeat", "Reply to server ping with {\"type\":\"pong\"}. Client ping receives pong."},
        {"reconnect", "Reconnect with exponential backoff and jitter, capped at 30 seconds. Send hello, wait for welcome, then GET /timers, /pulse all pages of /events, and /daily-tasks. Continue processing notifications during refresh."},
        {"delivery", "Timer, calendar, and daily-task hints go to all connected devices. Pulse events go only to device IDs subscribed through POST /pulse/subscriptions/{device_id}; DELETE unsubscribes. Default unsubscribed. Recipient membership is captured at dispatch; queued frames may still arrive after unsubscribe. No replay or acknowledgement. Refresh REST state after every welcome, reconnect, or uncertain connection. Do not treat event_id as a cursor; sequence is dispatch order."},
        {"limits", {{"clients", 128}, {"incoming_frame_bytes", 4096}, {"hello_timeout_seconds", 10}, {"outgoing_buffer_bytes", 262144}}},
        {"errors", json::array({"invalid_message", "invalid_hello", "unsupported_message", "frame_too_large"})}};
}
}
