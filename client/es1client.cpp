/*
 * If not stated otherwise in this file or this component's LICENSE file the
 * following copyright and licenses apply:
 *
 * Copyright 2024 Metrological
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ES1Bench on-device client.
//
// Standalone JSON-RPC benchmark client for the ES1Benchmark Thunder plugin,
// meant to run on the same box as Thunder (not over a separate machine like
// the Python scripts). Deliberately self-contained: plain POSIX sockets and
// hand-built JSON-RPC text, no dependency on WPEFramework's Core:: classes,
// so it has nothing else to link against and nothing else that can be out of
// sync with the plugin's own build.
//
// Configuration comes from a plain key=value file (default /opt/es1.config),
// since this is normally launched by systemd with no interactive arguments.
// See LoadConfig() below for the accepted keys.
//
// Output is JSON-Lines on stdout - one JSON object per test - so it can be
// redirected straight into a log file or piped into another tool.

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// ===========================================================================
// Small helpers
// ===========================================================================

static std::string Base64Encode(const uint8_t* data, size_t len) {
    static const char table[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((len + 2) / 3) * 4);
    size_t i = 0;
    while (i + 3 <= len) {
        uint32_t n = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
        out += table[(n >> 18) & 0x3F];
        out += table[(n >> 12) & 0x3F];
        out += table[(n >> 6) & 0x3F];
        out += table[n & 0x3F];
        i += 3;
    }
    size_t rem = len - i;
    if (rem == 1) {
        uint32_t n = data[i] << 16;
        out += table[(n >> 18) & 0x3F];
        out += table[(n >> 12) & 0x3F];
        out += "==";
    } else if (rem == 2) {
        uint32_t n = (data[i] << 16) | (data[i + 1] << 8);
        out += table[(n >> 18) & 0x3F];
        out += table[(n >> 12) & 0x3F];
        out += table[(n >> 6) & 0x3F];
        out += "=";
    }
    return out;
}

static std::string NowIso8601() {
    auto t = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", std::gmtime(&t));
    return std::string(buf);
}

static uint64_t NowSteadyUs() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Extract the raw text of the "result" value from a JSON-RPC response body,
// honoring nested brackets/braces and quoted strings, without needing a full
// JSON parser. Every payload this client sends/receives is plain ASCII with
// no embedded quotes/braces inside string values, which keeps this safe.
static std::string ExtractResultText(const std::string& resp) {
    size_t pos = resp.find("\"result\":");
    if (pos == std::string::npos) return "";
    pos += 9;
    while (pos < resp.size() && std::isspace(static_cast<unsigned char>(resp[pos]))) ++pos;
    if (pos >= resp.size()) return "";

    size_t start = pos;
    int depth = 0;
    bool inString = false;
    bool escaped = false;
    size_t i = pos;
    for (; i < resp.size(); ++i) {
        char c = resp[i];
        if (inString) {
            if (escaped) escaped = false;
            else if (c == '\\') escaped = true;
            else if (c == '"') inString = false;
            continue;
        }
        if (c == '"') { inString = true; continue; }
        if (c == '{' || c == '[') { ++depth; continue; }
        if (c == '}' || c == ']') {
            if (depth == 0) break;
            --depth; continue;
        }
        if (depth == 0 && c == ',') break;
    }
    return resp.substr(start, i - start);
}

static bool HasError(const std::string& resp) {
    return resp.find("\"error\":") != std::string::npos && resp.find("\"result\":") == std::string::npos;
}

// ===========================================================================
// Config
// ===========================================================================

struct Config {
    std::string mode        = "warm";     // coldstart | warm
    std::string transport   = "ws";       // ws | http
    std::string http_mode   = "session";  // session | oneshot  (transport=http only)
    std::string tier        = "single";   // single | multiple
    std::string size        = "5KB";      // used only when tier=single
    int iterations           = 20;
    int warmup               = 3;
    std::vector<int> clients = {1}; // comma-separated in the config, e.g. "32,8,1" for a full sweep
    bool skip_scalars        = false;
    std::string host         = "127.0.0.1";
    int port                  = 55555;
    int timeout_s             = 30;
    int coldstart_poll_ms     = 100;
    int coldstart_timeout_s   = 120;
};

static std::string Trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

// Returns false (and leaves *cfgOut untouched) if the config file doesn't
// exist - the caller treats that as "do nothing" rather than falling back
// to built-in defaults, since this is normally launched unattended by
// systemd and silently running with guessed defaults isn't wanted.
static bool LoadConfig(const std::string& path, Config* cfgOut) {
    Config cfg;
    std::ifstream in(path);
    if (!in.is_open()) {
        return false;
    }
    std::string line;
    while (std::getline(in, line)) {
        std::string t = Trim(line);
        if (t.empty() || t[0] == '#') continue;
        size_t eq = t.find('=');
        if (eq == std::string::npos) continue;
        std::string key = Trim(t.substr(0, eq));
        std::string val = Trim(t.substr(eq + 1));
        if (key == "mode") cfg.mode = val;
        else if (key == "transport") cfg.transport = val;
        else if (key == "http_mode") cfg.http_mode = val;
        else if (key == "tier") cfg.tier = val;
        else if (key == "size") cfg.size = val;
        else if (key == "iterations") cfg.iterations = std::stoi(val);
        else if (key == "warmup") cfg.warmup = std::stoi(val);
        else if (key == "clients") {
            cfg.clients.clear();
            std::stringstream ss(val);
            std::string tok;
            while (std::getline(ss, tok, ',')) {
                std::string trimmed = Trim(tok);
                if (!trimmed.empty()) cfg.clients.push_back(std::stoi(trimmed));
            }
            if (cfg.clients.empty()) cfg.clients.push_back(1);
        }
        else if (key == "skip_scalars") cfg.skip_scalars = (val == "true" || val == "1");
        else if (key == "host") cfg.host = val;
        else if (key == "port") cfg.port = std::stoi(val);
        else if (key == "timeout_s") cfg.timeout_s = std::stoi(val);
        else if (key == "coldstart_poll_ms") cfg.coldstart_poll_ms = std::stoi(val);
        else if (key == "coldstart_timeout_s") cfg.coldstart_timeout_s = std::stoi(val);
    }
    *cfgOut = cfg;
    return true;
}

// ===========================================================================
// Tiers and payload generation (mirrors the Python scripts' _build_exact_payload)
// ===========================================================================

static const uint32_t ARRAY_LIMIT = 256 * 1024;

static long ParseSize(const std::string& s) {
    std::string u = s;
    for (auto& c : u) c = std::toupper(static_cast<unsigned char>(c));
    if (u.size() > 2 && u.compare(u.size() - 2, 2, "KB") == 0)
        return std::stol(u.substr(0, u.size() - 2)) * 1024L;
    if (u.size() > 2 && u.compare(u.size() - 2, 2, "MB") == 0)
        return std::stol(u.substr(0, u.size() - 2)) * 1024L * 1024L;
    if (u.size() > 1 && u.back() == 'B')
        return std::stol(u.substr(0, u.size() - 1));
    return std::stol(u);
}

static const std::vector<std::pair<std::string, long>> kAllTiers = {
    {"500B", 500}, {"5KB", 5L * 1024}, {"50KB", 50L * 1024}, {"150KB", 150L * 1024},
    {"500KB", 500L * 1024}, {"1MB", 1L * 1024 * 1024}, {"4MB", 4L * 1024 * 1024},
};

static std::string MakeArrayElement(long i) { return std::to_string(i % 256); }

static std::string MakeMixedElement(long i) {
    char buf[160];
    std::snprintf(buf, sizeof(buf),
        "{\"id\":%ld,\"name\":\"item%ld\",\"value\":%.4f,\"flag\":%s}",
        i, i, i * 3.14159, (i % 2 == 0) ? "true" : "false");
    return buf;
}

static std::string MakeNestedElement(long i) {
    char buf[320];
    std::snprintf(buf, sizeof(buf),
        "{\"id\":%ld,\"flag\":%s,\"score\":%.2f,\"data\":{\"label\":\"item%ld\","
        "\"nested\":{\"count\":%ld,\"inner\":{\"value\":%ld,\"name\":\"deep%ld\"}}}}",
        i, (i % 2 == 0) ? "true" : "false", i * 3.14159, i, i, i * 3, i);
    return buf;
}

// Builds a JSON array text whose length is as close to targetBytes as
// possible, refining an avg-bytes-per-element estimate element-by-element -
// same strategy as the Python _build_exact_payload, so a C++ Set* client and
// a Python one requesting the "same" tier send comparably-sized payloads.
static std::string BuildExactArrayPayload(long targetBytes, const std::string& kind,
                                           long maxCount, long* outCount) {
    double avg = (kind == "array") ? 4.57 : (kind == "mixed") ? 62.0 : 151.0;
    long count = std::min<long>(maxCount, std::max<long>(1, static_cast<long>(targetBytes / avg)));

    auto makeElem = [&](long i) -> std::string {
        if (kind == "array") return MakeArrayElement(i);
        if (kind == "mixed") return MakeMixedElement(i);
        return MakeNestedElement(i);
    };

    std::vector<std::string> elems;
    elems.reserve(count);
    for (long i = 0; i < count; ++i) elems.push_back(makeElem(i));

    auto currentLen = [&]() {
        long total = 2; // "[" + "]"
        for (size_t k = 0; k < elems.size(); ++k) {
            total += static_cast<long>(elems[k].size());
            if (k + 1 < elems.size()) total += 1; // ","
        }
        return total;
    };

    long current = currentLen();
    long i = static_cast<long>(elems.size());
    while (current < targetBytes && i < maxCount) {
        std::string e = makeElem(i);
        current += static_cast<long>(e.size()) + (elems.empty() ? 0 : 1);
        elems.push_back(std::move(e));
        ++i;
    }
    while (current > targetBytes && elems.size() > 1) {
        current -= static_cast<long>(elems.back().size()) + 1;
        elems.pop_back();
    }

    if (outCount) *outCount = static_cast<long>(elems.size());

    std::string out = "[";
    for (size_t k = 0; k < elems.size(); ++k) {
        out += elems[k];
        if (k + 1 < elems.size()) out += ",";
    }
    out += "]";
    return out;
}

static long CountForTier(long targetBytes, const std::string& kind, long maxCount) {
    long count = 0;
    BuildExactArrayPayload(targetBytes, kind, maxCount, &count);
    return count;
}

static std::string GenerateStringPayload(long n) { return std::string(n, 'A'); }

// ===========================================================================
// Raw sockets
// ===========================================================================

static int ConnectTcp(const std::string& host, int port, int timeoutS, std::string* err) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { if (err) *err = "socket() failed"; return -1; }

    struct timeval tv { timeoutS, 0 };
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        if (err) *err = "invalid host address";
        ::close(fd);
        return -1;
    }
    if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        if (err) *err = "connect() failed";
        ::close(fd);
        return -1;
    }
    return fd;
}

static bool SendAll(int fd, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, data + sent, len - sent, 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static ssize_t RecvSome(int fd, char* buf, size_t len) {
    return ::recv(fd, buf, len, 0);
}

// ===========================================================================
// Minimal RFC6455 WebSocket client
// ===========================================================================

class WsClient {
public:
    bool Connect(const std::string& host, int port, int timeoutS) {
        std::string err;
        fd_ = ConnectTcp(host, port, timeoutS, &err);
        if (fd_ < 0) { lastError_ = err; return false; }
        return DoHandshake(host, port);
    }

    void Close() {
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    }

    ~WsClient() { Close(); }

    bool SendText(const std::string& payload) {
        return SendFrame(0x1, payload);
    }

    // Reads one complete WebSocket text message, transparently continuing
    // across fragmented frames and answering any ping with a pong.
    bool RecvText(std::string* out) {
        out->clear();
        for (;;) {
            uint8_t hdr[2];
            if (!RecvExact(reinterpret_cast<char*>(hdr), 2)) return false;
            bool fin = (hdr[0] & 0x80) != 0;
            uint8_t opcode = hdr[0] & 0x0F;
            bool masked = (hdr[1] & 0x80) != 0; // servers should not mask, but tolerate it
            uint64_t len = hdr[1] & 0x7F;

            if (len == 126) {
                uint8_t ext[2];
                if (!RecvExact(reinterpret_cast<char*>(ext), 2)) return false;
                len = (static_cast<uint16_t>(ext[0]) << 8) | ext[1];
            } else if (len == 127) {
                uint8_t ext[8];
                if (!RecvExact(reinterpret_cast<char*>(ext), 8)) return false;
                len = 0;
                for (int k = 0; k < 8; ++k) len = (len << 8) | ext[k];
            }

            uint8_t maskKey[4] = {0, 0, 0, 0};
            if (masked && !RecvExact(reinterpret_cast<char*>(maskKey), 4)) return false;

            std::string payload;
            payload.resize(static_cast<size_t>(len));
            if (len > 0 && !RecvExact(&payload[0], static_cast<size_t>(len))) return false;
            if (masked) {
                for (size_t k = 0; k < payload.size(); ++k)
                    payload[k] = static_cast<char>(static_cast<uint8_t>(payload[k]) ^ maskKey[k % 4]);
            }

            if (opcode == 0x9) { // ping -> pong
                SendFrame(0xA, payload);
                continue;
            }
            if (opcode == 0xA) continue; // unsolicited pong, ignore
            if (opcode == 0x8) return false; // close

            out->append(payload);
            if (fin) return true;
            // otherwise it's a continuation fragment - loop for the next frame
        }
    }

    const std::string& LastError() const { return lastError_; }

private:
    bool DoHandshake(const std::string& host, int port) {
        std::mt19937 rng(static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count()));
        uint8_t keyBytes[16];
        for (auto& b : keyBytes) b = static_cast<uint8_t>(rng() & 0xFF);
        std::string key = Base64Encode(keyBytes, sizeof(keyBytes));

        std::ostringstream req;
        req << "GET /jsonrpc HTTP/1.1\r\n"
            << "Host: " << host << ":" << port << "\r\n"
            << "Upgrade: websocket\r\n"
            << "Connection: Upgrade\r\n"
            << "Sec-WebSocket-Key: " << key << "\r\n"
            << "Sec-WebSocket-Version: 13\r\n\r\n";
        std::string reqStr = req.str();
        if (!SendAll(fd_, reqStr.data(), reqStr.size())) { lastError_ = "handshake send failed"; return false; }

        std::string headerBuf;
        char buf[512];
        for (;;) {
            ssize_t n = RecvSome(fd_, buf, sizeof(buf));
            if (n <= 0) { lastError_ = "handshake recv failed"; return false; }
            headerBuf.append(buf, static_cast<size_t>(n));
            if (headerBuf.find("\r\n\r\n") != std::string::npos) break;
            if (headerBuf.size() > 16384) { lastError_ = "handshake response too large"; return false; }
        }
        if (headerBuf.find("101") == std::string::npos) {
            lastError_ = "server did not upgrade to websocket";
            return false;
        }
        // Accept-key validation intentionally skipped (see file header note) -
        // this is a benchmarking tool, not a protocol conformance test.
        return true;
    }

    bool SendFrame(uint8_t opcode, const std::string& payload) {
        std::string frame;
        frame.push_back(static_cast<char>(0x80 | opcode)); // FIN=1

        uint64_t len = payload.size();
        if (len < 126) {
            frame.push_back(static_cast<char>(0x80 | len)); // MASK=1
        } else if (len <= 0xFFFF) {
            frame.push_back(static_cast<char>(0x80 | 126));
            frame.push_back(static_cast<char>((len >> 8) & 0xFF));
            frame.push_back(static_cast<char>(len & 0xFF));
        } else {
            frame.push_back(static_cast<char>(0x80 | 127));
            for (int shift = 56; shift >= 0; shift -= 8)
                frame.push_back(static_cast<char>((len >> shift) & 0xFF));
        }

        std::mt19937 rng(static_cast<unsigned>(std::chrono::steady_clock::now().time_since_epoch().count()));
        uint8_t maskKey[4];
        for (auto& b : maskKey) b = static_cast<uint8_t>(rng() & 0xFF);
        frame.append(reinterpret_cast<char*>(maskKey), 4);

        size_t start = frame.size();
        frame.resize(start + payload.size());
        for (size_t k = 0; k < payload.size(); ++k)
            frame[start + k] = static_cast<char>(static_cast<uint8_t>(payload[k]) ^ maskKey[k % 4]);

        return SendAll(fd_, frame.data(), frame.size());
    }

    bool RecvExact(char* buf, size_t len) {
        size_t got = 0;
        while (got < len) {
            ssize_t n = RecvSome(fd_, buf + got, len - got);
            if (n <= 0) return false;
            got += static_cast<size_t>(n);
        }
        return true;
    }

    int fd_ = -1;
    std::string lastError_;
};

// ===========================================================================
// Minimal HTTP/1.1 JSON-RPC client (session + oneshot)
// ===========================================================================

class HttpClient {
public:
    HttpClient(std::string host, int port, int timeoutS, bool oneShot)
        : host_(std::move(host)), port_(port), timeoutS_(timeoutS), oneShot_(oneShot) {}

    ~HttpClient() { Close(); }

    void Close() {
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
    }

    // Sends one JSON-RPC POST and returns the response body (empty on failure).
    bool Post(const std::string& jsonBody, std::string* respBody) {
        if (oneShot_ || fd_ < 0) {
            Close();
            std::string err;
            fd_ = ConnectTcp(host_, port_, timeoutS_, &err);
            if (fd_ < 0) return false;
        }

        std::ostringstream req;
        req << "POST /jsonrpc HTTP/1.1\r\n"
            << "Host: " << host_ << ":" << port_ << "\r\n"
            << "Content-Type: application/json\r\n"
            << "Content-Length: " << jsonBody.size() << "\r\n"
            << "Connection: " << (oneShot_ ? "close" : "keep-alive") << "\r\n\r\n"
            << jsonBody;
        std::string reqStr = req.str();
        if (!SendAll(fd_, reqStr.data(), reqStr.size())) { Close(); return false; }

        std::string buf;
        char chunk[4096];
        size_t headerEnd = std::string::npos;
        long contentLength = -1;
        for (;;) {
            ssize_t n = RecvSome(fd_, chunk, sizeof(chunk));
            if (n <= 0) { Close(); return false; }
            buf.append(chunk, static_cast<size_t>(n));
            if (headerEnd == std::string::npos) {
                headerEnd = buf.find("\r\n\r\n");
                if (headerEnd != std::string::npos) {
                    std::string headers = buf.substr(0, headerEnd);
                    size_t clPos = headers.find("Content-Length:");
                    if (clPos == std::string::npos) clPos = headers.find("content-length:");
                    if (clPos != std::string::npos) {
                        size_t lineEnd = headers.find("\r\n", clPos);
                        std::string val = headers.substr(clPos, lineEnd - clPos);
                        size_t colon = val.find(':');
                        contentLength = std::stol(Trim(val.substr(colon + 1)));
                    }
                }
            }
            if (headerEnd != std::string::npos && contentLength >= 0) {
                size_t haveBody = buf.size() - (headerEnd + 4);
                if (static_cast<long>(haveBody) >= contentLength) break;
            }
        }

        *respBody = buf.substr(headerEnd + 4, static_cast<size_t>(contentLength));
        if (oneShot_) Close();
        return true;
    }

private:
    std::string host_;
    int port_;
    int timeoutS_;
    bool oneShot_;
    int fd_ = -1;
};

// ===========================================================================
// JSON-RPC request building
// ===========================================================================

static std::atomic<long long> g_requestId{1};

static std::string BuildRequest(const std::string& method, const std::string& paramsJson) {
    std::ostringstream o;
    o << "{\"jsonrpc\":\"2.0\",\"id\":" << g_requestId.fetch_add(1)
      << ",\"method\":\"ES1Benchmark.1." << method << "\",\"params\":" << paramsJson << "}";
    return o.str();
}

// ===========================================================================
// Stats
// ===========================================================================

struct Stats {
    double minMs = 0, maxMs = 0, avgMs = 0, stddevMs = 0;
    int samples = 0;
    int skipped = 0;
};

static Stats ComputeStats(const std::vector<double>& roundtripSeconds, int skipped) {
    Stats s;
    s.skipped = skipped;
    if (roundtripSeconds.empty()) return s;
    std::vector<double> ms;
    ms.reserve(roundtripSeconds.size());
    for (double v : roundtripSeconds) ms.push_back(v * 1000.0);
    s.samples = static_cast<int>(ms.size());
    s.minMs = *std::min_element(ms.begin(), ms.end());
    s.maxMs = *std::max_element(ms.begin(), ms.end());
    double sum = 0; for (double v : ms) sum += v;
    s.avgMs = sum / ms.size();
    if (ms.size() > 1) {
        double sq = 0;
        for (double v : ms) sq += (v - s.avgMs) * (v - s.avgMs);
        s.stddevMs = std::sqrt(sq / (ms.size() - 1));
    }
    return s;
}

// ===========================================================================
// Barrier (resettable, for synchronizing concurrent client threads)
// ===========================================================================

class Barrier {
public:
    explicit Barrier(unsigned count) : count_(count), initial_(count), generation_(0) {}
    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        unsigned gen = generation_;
        if (--count_ == 0) {
            ++generation_;
            count_ = initial_;
            cv_.notify_all();
        } else {
            cv_.wait(lock, [this, gen] { return gen != generation_; });
        }
    }
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    unsigned count_;
    unsigned initial_;
    unsigned generation_;
};

// ===========================================================================
// Test definitions
// ===========================================================================

struct TestCase {
    std::string method;
    std::string paramsJson;
    std::string tierLabel;
    long targetBytes = 0;      // 0 for scalar tests
    bool isGet = false;
    std::string measureMethod; // matching Measure* calibration call, if any
    std::string measureParamsJson;
};

static std::vector<TestCase> BuildTierTests(const std::string& tierLabel, long size) {
    std::vector<TestCase> tests;

    tests.push_back({"setstring", "{\"value\":\"" + GenerateStringPayload(size) + "\"}", tierLabel, size, false, "", ""});
    {
        char p[64]; std::snprintf(p, sizeof(p), "{\"size\":%ld}", size);
        char mp[64]; std::snprintf(mp, sizeof(mp), "{\"size\":%ld}", size);
        tests.push_back({"getstring", p, tierLabel, size, true, "measurestringresizecost", mp});
    }

    if (size <= ARRAY_LIMIT) {
        long arrCount = CountForTier(size, "array", 262144);
        long mixedCount = CountForTier(size, "mixed", 4228);
        long nestedCount = CountForTier(size, "nested", 1736);

        std::string arrPayload = BuildExactArrayPayload(size, "array", 262144, nullptr);
        std::string mixedPayload = BuildExactArrayPayload(size, "mixed", 4228, nullptr);
        std::string nestedPayload = BuildExactArrayPayload(size, "nested", 1736, nullptr);

        tests.push_back({"setarray", "{\"value\":" + arrPayload + "}", tierLabel, size, false, "", ""});
        {
            char p[64]; std::snprintf(p, sizeof(p), "{\"size\":%ld}", arrCount);
            char mp[64]; std::snprintf(mp, sizeof(mp), "{\"size\":%ld}", arrCount);
            tests.push_back({"getarray", p, tierLabel, size, true, "measurecopycost", mp});
        }

        tests.push_back({"setmixedarray", "{\"value\":" + mixedPayload + "}", tierLabel, size, false, "", ""});
        {
            char p[64]; std::snprintf(p, sizeof(p), "{\"count\":%ld}", mixedCount);
            char mp[64]; std::snprintf(mp, sizeof(mp), "{\"count\":%ld}", mixedCount);
            tests.push_back({"getmixedarray", p, tierLabel, size, true, "measuremixedassigncost", mp});
        }

        tests.push_back({"setnestedobjects", "{\"value\":" + nestedPayload + "}", tierLabel, size, false, "", ""});
        {
            char p[64]; std::snprintf(p, sizeof(p), "{\"count\":%ld}", nestedCount);
            char mp[64]; std::snprintf(mp, sizeof(mp), "{\"count\":%ld}", nestedCount);
            tests.push_back({"getnestedobjects", p, tierLabel, size, true, "measurenestedassigncost", mp});
        }
    }

    return tests;
}

static std::vector<TestCase> BuildScalarTests() {
    return {
        {"setint64", "{\"value\":18446744073709551615}", "scalar", 0, false, "", ""},
        {"getint64", "{}", "scalar", 0, true, "", ""},
    };
}

// ===========================================================================
// Runner
// ===========================================================================

struct ClientResult {
    // Exactly cfg.iterations entries, index-aligned with round number: a
    // skipped/errored round stores -1 rather than being omitted, so index i
    // always means "round i" the same way across every client - needed for
    // RunTest's per-round max-aggregation across clients to line up correctly
    // when some client hits an error partway through a multi-client run.
    std::vector<double> roundtripSeconds;
    int skipped = 0;
    std::string firstRequest;
    std::string firstResponse;
};

static ClientResult RunOneClient(const Config& cfg, const TestCase& test, Barrier* barrier) {
    ClientResult result;

    std::unique_ptr<WsClient> ws;
    std::unique_ptr<HttpClient> http;
    bool oneShot = (cfg.transport == "http" && cfg.http_mode == "oneshot");

    if (cfg.transport == "ws") {
        ws.reset(new WsClient());
        if (!ws->Connect(cfg.host, cfg.port, cfg.timeout_s)) {
            std::cerr << "[es1client] WS connect failed: " << ws->LastError() << "\n";
            result.skipped = cfg.iterations;
            result.roundtripSeconds.assign(cfg.iterations, -1.0);
            return result;
        }
    } else {
        http.reset(new HttpClient(cfg.host, cfg.port, cfg.timeout_s, oneShot));
    }

    std::string request = BuildRequest(test.method, test.paramsJson);

    auto doCall = [&](std::string* resp) -> bool {
        if (ws) return ws->SendText(request) && ws->RecvText(resp);
        return http->Post(request, resp);
    };

    for (int i = 0; i < cfg.warmup; ++i) {
        std::string resp;
        doCall(&resp);
    }
    if (barrier) barrier->Wait();

    result.firstRequest = request;

    for (int i = 0; i < cfg.iterations; ++i) {
        if (barrier) barrier->Wait();

        uint64_t t0 = NowSteadyUs();
        std::string resp;
        bool ok = doCall(&resp);
        uint64_t t1 = NowSteadyUs();

        if (!ok || HasError(resp)) {
            ++result.skipped;
            result.roundtripSeconds.push_back(-1.0); // placeholder, keeps round alignment
        } else {
            result.roundtripSeconds.push_back((t1 - t0) / 1e6);
            if (result.firstResponse.empty()) result.firstResponse = resp;
        }
        if (barrier) barrier->Wait();
    }

    return result;
}

static double RunMeasureCalibration(const Config& cfg, const std::string& measureMethod,
                                     const std::string& measureParams) {
    std::unique_ptr<WsClient> ws;
    std::unique_ptr<HttpClient> http;
    if (cfg.transport == "ws") {
        ws.reset(new WsClient());
        if (!ws->Connect(cfg.host, cfg.port, cfg.timeout_s)) return -1;
    } else {
        http.reset(new HttpClient(cfg.host, cfg.port, cfg.timeout_s, /*oneShot=*/false));
    }
    std::string req = BuildRequest(measureMethod, measureParams);
    std::string resp;
    bool ok = ws ? (ws->SendText(req) && ws->RecvText(&resp)) : http->Post(req, &resp);
    if (!ok || HasError(resp)) return -1;
    std::string val = ExtractResultText(resp);
    if (val.empty()) return -1;
    try { return std::stod(val); } catch (...) { return -1; }
}

static void RunTest(const Config& cfg, const TestCase& test, int clientCount) {
    std::vector<ClientResult> results(clientCount);
    if (clientCount <= 1) {
        results[0] = RunOneClient(cfg, test, nullptr);
    } else {
        Barrier barrier(static_cast<unsigned>(clientCount));
        std::vector<std::thread> threads;
        for (int c = 0; c < clientCount; ++c) {
            threads.emplace_back([&, c] { results[c] = RunOneClient(cfg, test, &barrier); });
        }
        for (auto& t : threads) t.join();
    }

    // Aggregate: one round-trip sample per round = max across clients for that round
    // (the round only completes once the slowest client finishes), matching the
    // Python matrix runner's aggregate_results_by_round.
    std::vector<double> aggregated;
    int totalSkipped = 0;
    size_t rounds = cfg.iterations;
    for (auto& r : results) totalSkipped += r.skipped;
    for (size_t i = 0; i < rounds; ++i) {
        double worst = -1;
        bool any = false;
        for (auto& r : results) {
            if (i < r.roundtripSeconds.size() && r.roundtripSeconds[i] >= 0) {
                any = true;
                worst = std::max(worst, r.roundtripSeconds[i]);
            }
        }
        if (any) aggregated.push_back(worst);
    }

    Stats stats = ComputeStats(aggregated, totalSkipped);

    std::string firstReq = results[0].firstRequest;
    std::string firstResp = results[0].firstResponse;

    std::string resultText = ExtractResultText(firstResp);
    long actualBytes = static_cast<long>(resultText.size());
    long targetBytes = test.targetBytes;

    double measureUs = -1;
    if (test.isGet && !test.measureMethod.empty() && !firstResp.empty()) {
        measureUs = RunMeasureCalibration(cfg, test.measureMethod, test.measureParamsJson);
    }

    // ---- JSONL output record ----
    std::ostringstream out;
    out << "{"
        << "\"ts\":\"" << NowIso8601() << "\","
        << "\"transport\":\"" << cfg.transport << "\","
        << "\"mode\":\"" << (cfg.transport == "http" ? cfg.http_mode : std::string("n/a")) << "\","
        << "\"method\":\"" << test.method << "\","
        << "\"tier\":\"" << test.tierLabel << "\","
        << "\"clients\":" << clientCount << ","
        << "\"iterations\":" << cfg.iterations << ","
        << "\"warmup\":" << cfg.warmup << ","
        << "\"wire_request_bytes\":" << firstReq.size() << ",";

    if (test.isGet && targetBytes > 0) {
        double ratio = targetBytes ? (static_cast<double>(actualBytes) / targetBytes) : 0.0;
        const char* verdict = (ratio >= 0.7 && ratio <= 1.3) ? "OK" : "WARN";
        out << "\"size_check\":{\"target_bytes\":" << targetBytes
            << ",\"actual_bytes\":" << actualBytes
            << ",\"ratio\":" << ratio
            << ",\"verdict\":\"" << verdict << "\"},";
    }

    out << "\"roundtrip_ms\":{\"min\":" << stats.minMs << ",\"max\":" << stats.maxMs
        << ",\"avg\":" << stats.avgMs << ",\"stddev\":" << stats.stddevMs << "},"
        << "\"samples\":" << stats.samples << ","
        << "\"skipped\":" << stats.skipped;

    if (measureUs >= 0) {
        out << ",\"measure_us\":" << static_cast<long long>(measureUs);
    }
    out << "}";

    std::cout << out.str() << std::endl;
}

// ===========================================================================
// Cold-start gate: wait until the ES1Benchmark JSON-RPC method is ready
// ===========================================================================

static bool IsBenchmarkReady(const Config& cfg) {
    std::string request = BuildRequest("getint64", "{}");
    std::string response;

    if (cfg.transport == "ws") {
        WsClient ws;
        return ws.Connect(cfg.host, cfg.port, /*timeoutS=*/1)
            && ws.SendText(request)
            && ws.RecvText(&response)
            && !HasError(response)
            && !ExtractResultText(response).empty();
    }

    HttpClient http(cfg.host, cfg.port, /*timeoutS=*/1, /*oneShot=*/true);
    return http.Post(request, &response)
        && !HasError(response)
        && !ExtractResultText(response).empty();
}

static bool WaitForThunderReady(const Config& cfg, double* bootToReadySeconds) {
    auto start = std::chrono::steady_clock::now();
    auto deadline = start + std::chrono::seconds(cfg.coldstart_timeout_s);
    while (std::chrono::steady_clock::now() < deadline) {
        if (IsBenchmarkReady(cfg)) {
            *bootToReadySeconds = std::chrono::duration<double>(
                std::chrono::steady_clock::now() - start).count();
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg.coldstart_poll_ms));
    }
    return false;
}

// ===========================================================================
// main
// ===========================================================================

int main(int argc, char** argv) {
    std::string configPath = (argc > 1) ? argv[1] : "/opt/es1.config";
    Config cfg;
    if (!LoadConfig(configPath, &cfg)) {
        // No config present - do nothing rather than run with guessed
        // defaults. Not an error: e.g. es1bench-coldstart.service runs on
        // every boot, and most images won't have dropped a config in place.
        return 0;
    }

    if (cfg.mode == "coldstart") {
        double bootToReady = 0;
        std::cerr << "[es1client] Waiting for ES1Benchmark at " << cfg.host << ":" << cfg.port << " ...\n";
        if (!WaitForThunderReady(cfg, &bootToReady)) {
            std::cerr << "[es1client] ES1Benchmark never became ready within "
                      << cfg.coldstart_timeout_s << "s - aborting.\n";
            return 1;
        }
        std::cout << "{\"event\":\"coldstart_ready\",\"boot_to_ready_seconds\":" << bootToReady << "}" << std::endl;
    }

    std::vector<TestCase> tests;

    if (!cfg.skip_scalars) {
        auto scalars = BuildScalarTests();
        tests.insert(tests.end(), scalars.begin(), scalars.end());
    }

    if (cfg.tier == "multiple") {
        for (auto& t : kAllTiers) {
            auto tierTests = BuildTierTests(t.first, t.second);
            tests.insert(tests.end(), tierTests.begin(), tierTests.end());
        }
    } else {
        long size = ParseSize(cfg.size);
        auto tierTests = BuildTierTests(cfg.size, size);
        tests.insert(tests.end(), tierTests.begin(), tierTests.end());
    }

    for (int clientCount : cfg.clients) {
        for (auto& t : tests) {
            RunTest(cfg, t, clientCount);
        }
    }

    return 0;
}
