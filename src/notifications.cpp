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
        std::optional<Identity> identity;
        bool ready = false, closing = false, initialized = false, pending = false, resume = false, caught_up = false;
        unsigned long long token = 0, cursor = 0, target = 0;
        unsigned deferred_pongs = 0;
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
    struct Request {
        unsigned long long token, cursor, through;
        std::string device_id;
        bool initialize, resume;
        Identity identity;
    };
    struct Result {
        unsigned long long token, cursor, boundary;
        bool initialize, resync;
        std::string frames;
    };
    std::deque<Request> requests;
    std::deque<Result> results;
    bool reset = false;
    unsigned long long next_token = 0;
    explicit Impl(std::shared_ptr<NotificationStore> s, NotificationOptions o) : store(std::move(s)), options(std::move(o)) {
        store->verify();

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
                const auto boundary = store->latest();
                std::deque<Request> jobs;
                { std::lock_guard<std::mutex> lock(mutex); jobs.swap(requests); }
                std::shared_ptr<const std::unordered_set<std::string>> subscribers;
                for (const auto& job : jobs) {
                    if (stopping) break;
                    const bool resync = job.initialize && (!job.resume || job.cursor > boundary);
                    Result result{job.token, resync ? boundary : job.cursor, boundary, job.initialize, resync, {}};
                    // Bound each page by both event count and bytes. A slow reader
                    // stops requesting pages rather than buffering its entire history.
                    std::optional<std::unordered_set<std::string>> actions;
                    for (const auto& event : store->after(result.cursor, job.initialize ? boundary : std::min(boundary,job.through))) {
                        const bool pulse = event["type"].get<std::string>().rfind("pulse.", 0) == 0;
                        if (pulse && !subscribers) subscribers = std::make_shared<const std::unordered_set<std::string>>(store->pulse_subscribers());
                        bool deliver = !pulse || subscribers->count(job.device_id);
                        if(event["type"]=="command.available") {
                            if(!actions)actions=store->device_actions(job.device_id);
                            const auto target=event.value("target_device",json(nullptr));
                            deliver=(target.is_null()||target==job.device_id)&&actions->count(event["action"].get<std::string>());
                        } else if(event["type"]=="command.updated") {
                            deliver=event.value("source_device",json(nullptr))==job.device_id || event.value("target_device",json(nullptr))==job.device_id || event.value("executing_device",json(nullptr))==job.device_id;
                        }
                        deliver = deliver && notification_permitted(job.identity,event);
                        const auto frame = deliver ? event.dump() + "\n" : std::string();
                        if (result.frames.size() + frame.size() > 65536) break;
                        result.frames += frame;
                        result.cursor = event["sequence"].get<unsigned long long>();
                    }
                    std::lock_guard<std::mutex> lock(mutex);
                    results.push_back(std::move(result));
                }
                if (failed) std::cerr << "Notification event polling recovered\n";
                failed = false;
            } catch (const std::exception& exception) {
                if (!failed) std::cerr << "Notification event polling will retry: " << exception.what() << '\n';
                // Pending work may have failed part-way through. Drop all sessions;
                // their durable client cursors make retry safe after recovery.
                { std::lock_guard<std::mutex> lock(mutex); reset = true; requests.clear(); results.clear(); }
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
            const bool resume = packet.contains("after_sequence");
            if (type != "hello" || !packet.contains("device_id") || !packet["device_id"].is_string() ||
                !std::regex_match(packet["device_id"].get<std::string>(), std::regex("[A-Za-z0-9_-]{1,64}")) || packet.size() != (resume ? 3 : 2) + (packet.contains("token") ? 1 : 0) ||
                (resume && !packet["after_sequence"].is_number_unsigned())) {
                error(client, "invalid_hello"); return;
            }
            if(packet.contains("token")&&!packet["token"].is_string()){error(client,"invalid_hello");return;}
            client.identity=options.identity_provider->authenticate(packet.value("token",std::string{}));
            if(!client.identity){error(client,"unauthorized");return;}
            client.device_id = packet["device_id"].get<std::string>();
            if(!permitted(*client.identity,"notifications.read")||!acts_as(*client.identity,client.device_id)){error(client,"forbidden");return;}
            client.resume = resume;
            client.cursor = resume ? packet["after_sequence"].get<unsigned long long>() : 0;
            client.ready = true;
        } else if (type == "ping" && packet.size() == 1) {
            if(client.initialized)client.output += "{\"type\":\"pong\"}\n";
            else if(++client.deferred_pongs > 128)error(client,"unsupported_message");
        } else if (type != "pong" || packet.size() != 1) error(client, "unsupported_message");
        client.received = Clock::now();
    }
    json resources(const Client& client) const {
        json result=json::array();
        for(const std::string& path:std::vector<std::string>{"/timers", "/pulse", "/events", "/daily-tasks", "/projects", "/files", "/files/drives", "/files/directories", "/devices/"+client.device_id, "/devices/"+client.device_id+"/actions", "/commands?source_device="+client.device_id, "/commands?executing_device="+client.device_id}) {
            if(client.identity->kind=="open"){result.push_back(path);continue;}
            auto base=path.substr(0,path.find('?'));
            for(const auto& rule:route_policies())if(rule.method=="GET"&&std::regex_match(base,rule.pattern)){
                bool allowed=true;for(const auto& permission:rule.permissions)allowed=allowed&&permitted(*client.identity,permission);
                if(allowed)result.push_back(path);break;
            }
        }
        return result;
    }
    void serve() {
        std::vector<Client> clients;
        while (!stopping) {
            std::deque<Result> pages;
            bool disconnect = false;
            {
                std::lock_guard<std::mutex> lock(mutex);
                pages.swap(results);
                disconnect = reset;
                reset = false;
                if(disconnect) requests.clear();
            }
            if (disconnect) {
                for (auto& client : clients) close(client.fd);
                clients.clear();
            }
            for(auto& client:clients)if(client.identity&&!client.closing&&!options.identity_provider->current(*client.identity))error(client,"unauthorized");
            for (const auto& page : pages) for (auto& client : clients) {
                if(client.token != page.token || client.closing)continue;
                client.pending = false;
                if(page.initialize){
                    client.initialized = true;client.target = page.boundary;
                    client.output += json{{"type", "welcome"}, {"protocol_version", client.resume ? 2 : 1}, {"device_id", client.device_id},
                        {"resync_required", page.resync}, {"sequence", page.boundary},
                        {"resources", resources(client)},
                        {"heartbeat_interval_seconds", options.heartbeat.count() / 1000},
                        {"idle_timeout_seconds", options.idle_timeout.count() / 1000}}.dump() + "\n";
                    while(client.deferred_pongs){client.output += "{\"type\":\"pong\"}\n";--client.deferred_pongs;}
                }
                const auto previous = client.cursor;
                client.output += page.frames;
                client.cursor = page.cursor;
                if(client.resume && !client.caught_up && client.cursor >= client.target){
                    client.output += json{{"type","catchup_complete"},{"sequence",client.cursor}}.dump()+"\n";
                    client.caught_up = true;
                }else if(client.resume && client.cursor > previous){
                    client.output += json{{"type","checkpoint"},{"sequence",client.cursor}}.dump()+"\n";
                }
                if(client.output.size()>max_output){client.closing=true;client.output.clear();}
            }
            for(auto& client : clients)if(client.ready && !client.closing && !client.pending && client.output.size()<65536){
                std::lock_guard<std::mutex> lock(mutex);
                if(requests.size()>=max_clients)continue;
                requests.push_back({client.token,client.cursor,client.resume && client.initialized && !client.caught_up ? client.target : std::numeric_limits<unsigned long long>::max(),client.device_id,!client.initialized,client.resume,*client.identity});
                client.pending=true;
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
                if(client.identity&&!client.closing&&!options.identity_provider->current(*client.identity))error(client,"unauthorized");
                if (!dead && !client.output.empty() && (flags & POLLOUT)) {
                    const auto count = send(client.fd, client.output.data(), client.output.size(), MSG_NOSIGNAL);
                    if (count > 0) client.output.erase(0, count);
                    else if (count < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) dead = true;
                }
                if ((!client.ready && now - client.connected > options.hello_timeout) ||
                    now - client.received > options.idle_timeout || (client.closing && client.output.empty())) dead = true;
                if (client.initialized && !client.closing && now - client.pinged >= options.heartbeat) {
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
                    else { int keepalive = 1; setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)); clients.push_back(Client{fd}); clients.back().token=++next_token; }
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
    return {{"transport", "tcp"}, {"port", port}, {"protocol_version", 2}, {"encoding", "UTF-8 newline-delimited JSON"},
        {"authentication", "none"}, {"resume_hello", {{"type","hello"},{"device_id","watch-1"},{"after_sequence",42}}},
        {"catchup_complete", {{"type","catchup_complete"},{"sequence",55}}},
        {"checkpoint", {{"type","checkpoint"},{"sequence",55}}},
        {"hello", {{"type", "hello"}, {"device_id", "watch-1"}}},
        {"welcome", {{"type", "welcome"}, {"protocol_version", 1}, {"device_id", "watch-1"}, {"resync_required", true}, {"sequence",42},
                     {"resources", json::array({"/timers", "/pulse", "/events", "/daily-tasks", "/projects", "/files", "/files/drives", "/files/directories", "/devices/watch-1"})}, {"heartbeat_interval_seconds", 30}, {"idle_timeout_seconds", 90}}},
        {"commands", "command.available is filtered to devices currently advertising the action and matching optional target_device. command.updated goes to source, explicit target, and executor. On reconnect and after advertising, POST /devices/{id}/commands/claim until command:null (subject to local execution capacity); also poll periodically. Refresh requester/executor command history through paginated GET /commands. Claims are required before execution; notifications never authorize work."},
        {"notification", {{"sequence", 42}, {"event_id", 123}, {"type", "timer.created"}, {"resource", "/timers/<id>"}, {"source_device", "watch-1"}}},
        {"event_types", json::array({"timer.created", "timer.expired", "timer.confirmed", "timer.stopped", "pulse.updated", "pulse.triggered", "calendar.created", "calendar.updated", "calendar.deleted", "calendar.reminder", "daily_tasks.updated", "daily_tasks.reset", "board.created", "board.updated", "board.deleted", "file.created", "file.updated", "file.deleted", "files.directories_updated", "drives.updated", "project.created", "project.updated", "project.deleted", "device.created", "device.updated", "device.deleted", "device.subscription_updated", "device.actions_updated", "command.available", "command.updated"})},
        {"change_notifications", {
            {"file.created", "Completed durable upload; resource=/files/{id}. Fetch metadata and refresh lists."},
            {"file.updated", "Text save or move; resource=/files/{id}. Refresh metadata/content and cached directory listings."},
            {"file.deleted", "Logical removal; resource=/files/{id} now returns 404. Includes project cascades; cleanup retries emit no duplicates."},
            {"drives.updated", "Drive registration, path or explicit attachment state changed; GET /files/drives for letters and current availability."},
            {"files.directories_updated", "Directory tree changed on drive_id, including parent creation or project-directory removal; resolve its letter through GET /files/drives and fetch GET /files/directories?drive={letter}."},
            {"project.created/updated/deleted", "Resource=/projects/{id}. Refresh lists and affected deep trees. Child membership and board changes update the parent/owning project; cascades notify every removed project."},
            {"device.created/updated/deleted", "Resource=/devices/{id}. Refresh/remove cached registration. source_device is null, not the affected identity."},
            {"device.subscription_updated", "Resource=/devices/{id}, plus boolean subscribed. Broadcast even to unsubscribed clients; unregistered IDs may return 404. Device deletion does not remove the independent subscription."},
            {"atomicity", "All changes and events commit together. No-op metadata/subscription requests emit nothing; each successful text save creates a revision and emits file.updated. New families have null source_device. Pre-upgrade changes are not backfilled."}
        }},
        {"heartbeat", "Reply to server ping with {\"type\":\"pong\"}. Client ping receives pong."},
        {"reconnect", "Reconnect with backoff and jitter capped at 30 seconds. Send optional after_sequence (unsigned 64-bit integer, last durably processed sequence, not event_id). Omit on first connection or when local state was lost. With a valid cursor, welcome protocol_version=2 and resync_required=false precedes ordered catch-up, catchup_complete, then live events. A cursor ahead of this database falls back to resync_required=true. Legacy hello gets protocol_version=1 and full REST resync. Welcome.sequence is a boundary, not permission to skip unprocessed events."},
        {"delivery", "Durable ordered replay uses sequence, never event_id. Persist cursors only after processing preceding notifications successfully; duplicates across reconnects are possible. checkpoint/catchup_complete permit advancing past subscription-filtered events. Pulse membership is checked when each page is read (current subscription also applies to history); queued frames may arrive after unsubscribe. REST remains authoritative: historical reminders are hints, not instructions to re-alert. No delivery or user acknowledgement; event history currently has no automatic pruning. If resync_required, refresh welcome.resources including all pages, directory trees for every registered drive, and project boards before saving the welcome boundary; continue consuming notifications during refresh."},
        {"limits", {{"clients", 128}, {"incoming_frame_bytes", 4096}, {"hello_timeout_seconds", 10}, {"outgoing_buffer_bytes", 262144}, {"page_events",256}, {"page_bytes",65536}}},
        {"errors", json::array({"invalid_message", "invalid_hello", "unsupported_message", "frame_too_large"})}};
}
}
