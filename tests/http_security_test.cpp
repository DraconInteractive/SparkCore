#include <httplib.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <thread>

static void check(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}

// Send literal wire bytes: normal HTTP clients may sanitize malformed requests.
static std::string exchange(int port, const std::string& request) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    check(fd >= 0, "socket");
    struct Close { int fd; ~Close() { close(fd); } } cleanup{fd};
    timeval timeout{3, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    check(connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "connect");
    size_t sent = 0;
    while (sent < request.size()) {
        auto n = send(fd, request.data() + sent, request.size() - sent, MSG_NOSIGNAL);
        check(n > 0, "send");
        sent += static_cast<size_t>(n);
    }
    std::string response;
    char buffer[4096];
    for (;;) {
        auto n = recv(fd, buffer, sizeof(buffer), 0);
        check(n >= 0, "response timed out");
        if (!n) break;
        response.append(buffer, static_cast<size_t>(n));
    }
    return response;
}

int main() {
    httplib::Server server;
    std::atomic<int> posts{0};
    server.Get("/header", [](const auto& req, auto& res) {
        res.set_content(req.get_header_value("X-Probe"), "text/plain");
    });
    server.Post("/body", [&](const auto& req, auto& res) {
        ++posts;
        res.set_content(req.get_header_value("Authorization") + "|" +
            req.get_header_value("Cookie") + "|" + req.get_header_value("Host") + "|" +
            std::to_string(req.get_header_value_count("Authorization")) + "|" + req.body, "text/plain");
    });
    int port = server.bind_to_any_port("127.0.0.1");
    if (port < 0) return 1;
    std::thread worker([&] { server.listen_after_bind(); });
    server.wait_until_ready();
    int result = 0;
    try {
        auto header = exchange(port, "GET /header HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nX-Probe: literal%0d%0aInjected:%20yes\r\n\r\n");
        check(header.find("\r\n\r\nliteral%0d%0aInjected:%20yes") != std::string::npos,
              "header values must not percent-decode CRLF");
        auto trailers = exchange(port, "POST /body HTTP/1.1\r\nHost: localhost\r\nAuthorization: original\r\nCookie: original-cookie\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nTrailer: Authorization, Cookie, Host\r\n\r\n1\r\nx\r\n0\r\nAuthorization: injected\r\nCookie: injected\r\nHost: injected\r\n\r\n");
        check(trailers.find("\r\n\r\noriginal|original-cookie|localhost|1|x") != std::string::npos,
              "trailers must not alter authentication or routing headers");
        int before = posts.load();
        auto negative = exchange(port, "POST /body HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n-1\r\nx\r\n0\r\n\r\n");
        check(negative.find("HTTP/1.1 400") == 0 && posts == before,
              "negative chunk size must be rejected before application handler");
        std::cout << "HTTP parser security regressions passed\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        result = 1;
    }
    server.stop();
    worker.join();
    return result;
}
