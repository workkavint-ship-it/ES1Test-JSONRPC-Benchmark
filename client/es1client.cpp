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
#include <dirent.h>
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
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <initializer_list>
#include <iostream>
#include <iterator>
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
    bool memory_measure       = true;
    int memory_sample_ms      = 10;
    bool result_file_enabled  = false;
    std::string result_file   = "/opt/result.json";
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
        else if (key == "memory_measure") cfg.memory_measure = (val == "true" || val == "1");
        else if (key == "memory_sample_ms") cfg.memory_sample_ms = std::max(1, std::stoi(val));
        else if (key == "result_file_enabled") cfg.result_file_enabled = (val == "true" || val == "1");
        else if (key == "result_file") cfg.result_file = val;
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
// Result file mirroring - in addition to stdout, optionally also persist
// every JSONL line to a file on disk (e.g. /opt/result.json), controlled by
// result_file_enabled/result_file in the config. Opened once in main() and
// truncated at startup, so the file always reflects the most recent run
// rather than growing forever across repeated invocations.
// ===========================================================================

static std::ofstream g_resultFile;

static void EmitLine(const std::string& line) {
    std::cout << line << std::endl;
    if (g_resultFile.is_open()) {
        g_resultFile << line << std::endl;
        g_resultFile.flush();
    }
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
    void Abort() {
        std::lock_guard<std::mutex> lock(mutex_);
        aborted_ = true;
        ++generation_;
        cv_.notify_all();
    }
    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (aborted_) return;
        unsigned gen = generation_;
        if (--count_ == 0) {
            ++generation_;
            count_ = initial_;
            cv_.notify_all();
        } else {
            cv_.wait(lock, [this, gen] { return aborted_ || gen != generation_; });
        }
    }
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    unsigned count_;
    unsigned initial_;
    unsigned generation_;
    bool aborted_ = false;
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

// ===========================================================================
// Automatic target memory measurement
// ===========================================================================

static std::string JsonEscape(const std::string& value) {
    std::string out;
    for (char c : value) {
        if (c == '\\' || c == '"') {
            out.push_back('\\');
            out.push_back(c);
        } else if (c == '\n') {
            out += "\\n";
        } else if (c == '\r') {
            out += "\\r";
        } else {
            out.push_back(c);
        }
    }
    return out;
}

static std::string ReadWholeFile(const std::string& path) {
    std::ifstream in(path, std::ios::in | std::ios::binary);
    if (!in.is_open()) return "";
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

static bool IsNumericPidName(const char* name) {
    if (name == nullptr || *name == '\0') return false;
    for (const char* p = name; *p != '\0'; ++p) {
        if (*p < '0' || *p > '9') return false;
    }
    return true;
}

static std::string ReadProcArgv0(int pid) {
    std::string cmdline = ReadWholeFile("/proc/" + std::to_string(pid) + "/cmdline");
    size_t nul = cmdline.find('\0');
    return Trim(nul == std::string::npos ? cmdline : cmdline.substr(0, nul));
}

// Matches on the exact basename of argv[0] rather than a substring search
// over the whole cmdline - a loose "contains WPEFramework" match can also
// hit wrapper scripts, log tools, or anything else that merely references
// the name in one of its arguments.
static bool IsWpeFrameworkPid(int pid) {
    std::string argv0 = ReadProcArgv0(pid);
    if (argv0.empty()) return false;
    size_t slash = argv0.find_last_of('/');
    std::string base = (slash == std::string::npos) ? argv0 : argv0.substr(slash + 1);
    return base == "WPEFramework";
}

// Parent PID from /proc/<pid>/stat, field 4. The comm field (field 2) is
// parenthesized and can itself contain spaces or parentheses, so the safe
// way to find the end of it is the *last* ')' in the line, not the first.
static int ReadProcPpid(int pid) {
    std::string stat = ReadWholeFile("/proc/" + std::to_string(pid) + "/stat");
    size_t lastParen = stat.find_last_of(')');
    if (lastParen == std::string::npos || lastParen + 2 >= stat.size()) return -1;
    std::istringstream rest(stat.substr(lastParen + 2));
    char state = 0;
    int ppid = -1;
    rest >> state >> ppid;
    return rest.fail() ? -1 : ppid;
}

static std::vector<int> CandidateWpeFrameworkPids() {
    std::vector<int> pids;
    std::ifstream pidFile("/tmp/wpeframework.pid");
    int pid = 0;
    if (pidFile >> pid && pid > 0) pids.push_back(pid);

    DIR* proc = ::opendir("/proc");
    if (proc == nullptr) return pids;
    while (dirent* entry = ::readdir(proc)) {
        if (IsNumericPidName(entry->d_name)) {
            int candidate = std::atoi(entry->d_name);
            if (candidate > 0 && std::find(pids.begin(), pids.end(), candidate) == pids.end()) {
                pids.push_back(candidate);
            }
        }
    }
    ::closedir(proc);
    return pids;
}

struct MemoryTarget {
    int pid = -1;
    std::string cgroupPath;
    std::string cgroupCurrentFile;
    std::string cgroupPeakFile;
    bool cgroupIsRoot = false;
};

static void ResolveCgroupFiles(int pid, MemoryTarget* target) {
    std::string cgroup = ReadWholeFile("/proc/" + std::to_string(pid) + "/cgroup");
    std::istringstream lines(cgroup);
    std::string line;
    while (std::getline(lines, line)) {
        size_t firstColon = line.find(':');
        size_t secondColon = line.find(':', firstColon == std::string::npos ? 0 : firstColon + 1);
        if (firstColon == std::string::npos || secondColon == std::string::npos) continue;

        std::string controllers = line.substr(firstColon + 1, secondColon - firstColon - 1);
        std::string path = line.substr(secondColon + 1);
        target->cgroupPath = path;
        target->cgroupIsRoot = (path.empty() || path == "/");

        std::vector<std::string> roots;
        if (controllers.empty()) {
            roots.push_back("/sys/fs/cgroup");
        } else if (controllers.find("memory") != std::string::npos) {
            roots.push_back("/sys/fs/cgroup/memory");
            roots.push_back("/sys/fs/cgroup");
        }

        for (const std::string& root : roots) {
            std::string base = root + path;
            std::string current = base + "/memory.current";
            std::string peak = base + "/memory.peak";
            if (std::ifstream(current).good()) {
                target->cgroupCurrentFile = current;
                target->cgroupPeakFile = peak;
                break;
            }
            current = base + "/memory.usage_in_bytes";
            peak = base + "/memory.max_usage_in_bytes";
            if (std::ifstream(current).good()) {
                target->cgroupCurrentFile = current;
                target->cgroupPeakFile = peak;
                break;
            }
        }
        if (!target->cgroupCurrentFile.empty()) break;
    }
}

// Picks one WPEFramework process to measure. /proc iteration order is not
// guaranteed, and out-of-process Thunder plugin hosts are frequently other
// copies of the same WPEFramework binary - so among every exact-basename
// match, prefer the one that is NOT itself a child of another match (an OOP
// host's parent is typically the main process), falling back to the lowest
// PID if that doesn't narrow it to one.
static bool DiscoverMemoryTarget(MemoryTarget* target, std::string* reason) {
    std::vector<int> matches;
    for (int pid : CandidateWpeFrameworkPids()) {
        if (IsWpeFrameworkPid(pid)) matches.push_back(pid);
    }
    if (matches.empty()) {
        if (reason) *reason = "WPEFramework process not found";
        return false;
    }

    int chosen = -1;
    for (int pid : matches) {
        int ppid = ReadProcPpid(pid);
        bool parentIsAlsoMatch = (ppid > 0) &&
            (std::find(matches.begin(), matches.end(), ppid) != matches.end());
        if (!parentIsAlsoMatch && (chosen < 0 || pid < chosen)) {
            chosen = pid;
        }
    }
    if (chosen < 0) {
        chosen = *std::min_element(matches.begin(), matches.end());
    }

    target->pid = chosen;
    ResolveCgroupFiles(chosen, target);
    return true;
}

static int64_t ReadIntegerFile(const std::string& path) {
    if (path.empty()) return -1;
    std::ifstream in(path);
    long long value = -1;
    if (!(in >> value) || value < 0) return -1;
    return static_cast<int64_t>(value);
}

// Scans one /proc file once, matching each line's prefix against every
// requested key in a single pass - avoids reopening and rescanning the same
// file once per key. This runs on a background thread every
// memory_sample_ms while the timed RPC loop is also running, so every extra
// file open/scan here is overhead sitting in that same window.
static void ReadKbFieldsMulti(const std::string& path,
                               std::initializer_list<std::pair<const char*, int64_t*>> fields,
                               bool toBytes) {
    std::ifstream in(path);
    if (!in.is_open()) return;
    std::string line;
    while (std::getline(in, line)) {
        for (auto& field : fields) {
            size_t keyLen = std::strlen(field.first);
            if (line.compare(0, keyLen, field.first) == 0) {
                std::istringstream value(line.substr(keyLen));
                long long kb = -1;
                if (value >> kb && kb >= 0) {
                    *field.second = toBytes ? static_cast<int64_t>(kb) * 1024 : static_cast<int64_t>(kb);
                }
                break;
            }
        }
    }
}

struct MemorySample {
    bool valid = false;
    int64_t rss = -1;
    int64_t vmSize = -1;
    int64_t vmPeak = -1;
    int64_t pss = -1;
    int64_t privateDirty = -1;
    int64_t anonymous = -1;
    int64_t cgroupCurrent = -1;
    int64_t cgroupPeak = -1;
    int64_t memTotalKb = -1;
    int64_t memFreeKb = -1;
    int64_t memAvailableKb = -1;
    int64_t buffersKb = -1;
    int64_t cachedKb = -1;
    int64_t swapTotalKb = -1;
    int64_t swapFreeKb = -1;
    std::string reason;
};

static MemorySample ReadMemorySample(const MemoryTarget& target) {
    MemorySample sample;
    if (!IsWpeFrameworkPid(target.pid)) {
        sample.reason = "WPEFramework PID disappeared or changed";
        return sample;
    }

    ReadKbFieldsMulti("/proc/" + std::to_string(target.pid) + "/status",
        {{"VmRSS:", &sample.rss}, {"VmSize:", &sample.vmSize}, {"VmPeak:", &sample.vmPeak}},
        /*toBytes=*/true);
    if (sample.rss < 0) {
        sample.reason = "VmRSS unavailable";
        return sample;
    }
    ReadKbFieldsMulti("/proc/" + std::to_string(target.pid) + "/smaps_rollup",
        {{"Pss:", &sample.pss}, {"Private_Dirty:", &sample.privateDirty}, {"Anonymous:", &sample.anonymous}},
        /*toBytes=*/true);
    sample.cgroupCurrent = ReadIntegerFile(target.cgroupCurrentFile);
    sample.cgroupPeak = ReadIntegerFile(target.cgroupPeakFile);
    ReadKbFieldsMulti("/proc/meminfo",
        {{"MemTotal:", &sample.memTotalKb}, {"MemFree:", &sample.memFreeKb},
         {"MemAvailable:", &sample.memAvailableKb}, {"Buffers:", &sample.buffersKb},
         {"Cached:", &sample.cachedKb}, {"SwapTotal:", &sample.swapTotalKb},
         {"SwapFree:", &sample.swapFreeKb}},
        /*toBytes=*/false);
    sample.valid = true;
    return sample;
}

static std::string MiB(int64_t bytes) {
    if (bytes < 0) return "null";
    std::ostringstream out;
    out << std::fixed << std::setprecision(3)
        << (static_cast<double>(bytes) / (1024.0 * 1024.0));
    return out.str();
}

static std::string MiBDouble(double bytes) {
    if (bytes < 0) return "null";
    std::ostringstream out;
    out << std::fixed << std::setprecision(3)
        << (bytes / (1024.0 * 1024.0));
    return out.str();
}

static std::string KbValue(int64_t kb) {
    return kb < 0 ? "null" : std::to_string(kb);
}

static std::string MiBFromKb(int64_t kb) {
    if (kb < 0) return "null";
    return MiBDouble(static_cast<double>(kb) * 1024.0);
}

static int64_t Delta(int64_t value, int64_t baseline) {
    return (value >= 0 && baseline >= 0) ? value - baseline : -1;
}

class MemoryMeasurement {
public:
    MemoryMeasurement(bool enabled, int sampleMs)
        : enabled_(enabled), sampleMs_(std::max(1, sampleMs)) {}

    void Begin() {
        if (!enabled_ || started_) return;
        started_ = true;
        if (!DiscoverMemoryTarget(&target_, &reason_)) return;
        baseline_ = ReadMemorySample(target_);
        if (!baseline_.valid) {
            reason_ = baseline_.reason;
            return;
        }
        peak_ = baseline_;
        running_ = true;
        sampler_ = std::thread([this] {
            while (running_) {
                Sample();
                std::this_thread::sleep_for(std::chrono::milliseconds(sampleMs_));
            }
        });
    }

    void End() {
        if (!started_ || !baseline_.valid) return;
        running_ = false;
        if (sampler_.joinable()) sampler_.join();
        Sample();
        end_ = last_;
    }

    std::string ToJson() const {
        std::ostringstream out;
        out << "{\"enabled\":" << (enabled_ ? "true" : "false");
        if (!enabled_) {
            out << "}";
            return out.str();
        }

        // Reflects only whether the memory sampling itself succeeded
        // (baseline and end snapshots both captured, no read failure along
        // the way) - independent of whether individual RPC calls in this
        // test were skipped. An unrelated call timeout doesn't affect
        // whether the RSS samples taken throughout the test are trustworthy,
        // and gating on it made 'valid' hardest to get exactly at high
        // concurrency, where memory data matters most. The top-level
        // "skipped" field already reports call failures separately.
        bool valid = baseline_.valid && end_.valid && reason_.empty();
        double average = sampleCount_ > 0
            ? static_cast<double>(sumRss_) / static_cast<double>(sampleCount_)
            : -1.0;
        out << ",\"available\":" << (baseline_.valid ? "true" : "false")
            << ",\"valid\":" << (valid ? "true" : "false")
            << ",\"pid\":" << target_.pid
            << ",\"cgroup\":\"" << JsonEscape(target_.cgroupPath) << "\""
            << ",\"cgroup_is_root\":" << (target_.cgroupIsRoot ? "true" : "false")
            << ",\"sample_interval_ms\":" << sampleMs_
            << ",\"sample_count\":" << sampleCount_
            << ",\"baseline_rss_mib\":" << MiB(baseline_.rss)
            << ",\"peak_rss_mib\":" << MiB(peak_.rss)
            << ",\"end_rss_mib\":" << MiB(end_.rss)
            << ",\"peak_delta_mib\":" << MiB(Delta(peak_.rss, baseline_.rss))
            << ",\"end_delta_mib\":" << MiB(Delta(end_.rss, baseline_.rss))
            << ",\"average_rss_mib\":" << MiBDouble(average)
            << ",\"baseline_vmsize_mib\":" << MiB(baseline_.vmSize)
            << ",\"peak_vmsize_mib\":" << MiB(peak_.vmSize)
            << ",\"end_vmsize_mib\":" << MiB(end_.vmSize)
            << ",\"peak_vmsize_delta_mib\":" << MiB(Delta(peak_.vmSize, baseline_.vmSize))
            << ",\"baseline_vmpeak_mib\":" << MiB(baseline_.vmPeak)
            << ",\"peak_vmpeak_mib\":" << MiB(peak_.vmPeak)
            << ",\"end_vmpeak_mib\":" << MiB(end_.vmPeak)
            << ",\"baseline_pss_mib\":" << MiB(baseline_.pss)
            << ",\"peak_pss_mib\":" << MiB(peak_.pss)
            << ",\"end_pss_mib\":" << MiB(end_.pss)
            << ",\"baseline_private_dirty_mib\":" << MiB(baseline_.privateDirty)
            << ",\"peak_private_dirty_mib\":" << MiB(peak_.privateDirty)
            << ",\"end_private_dirty_mib\":" << MiB(end_.privateDirty)
            << ",\"baseline_anonymous_mib\":" << MiB(baseline_.anonymous)
            << ",\"peak_anonymous_mib\":" << MiB(peak_.anonymous)
            << ",\"end_anonymous_mib\":" << MiB(end_.anonymous)
            << ",\"baseline_cgroup_current_mib\":" << MiB(baseline_.cgroupCurrent)
            << ",\"peak_cgroup_current_mib\":" << MiB(peak_.cgroupCurrent)
            << ",\"end_cgroup_current_mib\":" << MiB(end_.cgroupCurrent)
            << ",\"peak_cgroup_current_delta_mib\":"
            << MiB(Delta(peak_.cgroupCurrent, baseline_.cgroupCurrent))
            << ",\"baseline_cgroup_peak_mib\":" << MiB(baseline_.cgroupPeak)
            << ",\"peak_cgroup_peak_mib\":" << MiB(peak_.cgroupPeak)
            << ",\"end_cgroup_peak_mib\":" << MiB(end_.cgroupPeak)
            << ",\"cgroup_peak_delta_mib\":"
            << MiB(Delta(peak_.cgroupPeak, baseline_.cgroupPeak))
            << ",\"system_memory\":{\"baseline_mem_total_kb\":" << KbValue(baseline_.memTotalKb)
            << ",\"baseline_mem_free_kb\":" << KbValue(baseline_.memFreeKb)
            << ",\"baseline_mem_available_kb\":" << KbValue(baseline_.memAvailableKb)
            << ",\"baseline_buffers_kb\":" << KbValue(baseline_.buffersKb)
            << ",\"baseline_cached_kb\":" << KbValue(baseline_.cachedKb)
            << ",\"baseline_swap_total_kb\":" << KbValue(baseline_.swapTotalKb)
            << ",\"baseline_swap_free_kb\":" << KbValue(baseline_.swapFreeKb)
            << ",\"minimum_mem_free_kb\":" << KbValue(peak_.memFreeKb)
            << ",\"minimum_mem_available_kb\":" << KbValue(peak_.memAvailableKb)
            << ",\"minimum_swap_free_kb\":" << KbValue(peak_.swapFreeKb)
            << ",\"peak_buffers_kb\":" << KbValue(peak_.buffersKb)
            << ",\"peak_cached_kb\":" << KbValue(peak_.cachedKb)
            << ",\"end_mem_total_kb\":" << KbValue(end_.memTotalKb)
            << ",\"end_mem_free_kb\":" << KbValue(end_.memFreeKb)
            << ",\"end_mem_available_kb\":" << KbValue(end_.memAvailableKb)
            << ",\"end_buffers_kb\":" << KbValue(end_.buffersKb)
            << ",\"end_cached_kb\":" << KbValue(end_.cachedKb)
            << ",\"end_swap_total_kb\":" << KbValue(end_.swapTotalKb)
            << ",\"end_swap_free_kb\":" << KbValue(end_.swapFreeKb)
            << ",\"mem_free_drop_kb\":"
            << KbValue(Delta(baseline_.memFreeKb, peak_.memFreeKb))
            << ",\"mem_available_drop_kb\":"
            << KbValue(Delta(baseline_.memAvailableKb, peak_.memAvailableKb))
            << ",\"swap_free_drop_kb\":"
            << KbValue(Delta(baseline_.swapFreeKb, peak_.swapFreeKb))
            << ",\"baseline_mem_total_mib\":" << MiBFromKb(baseline_.memTotalKb)
            << ",\"baseline_mem_free_mib\":" << MiBFromKb(baseline_.memFreeKb)
            << ",\"baseline_mem_available_mib\":" << MiBFromKb(baseline_.memAvailableKb)
            << ",\"minimum_mem_free_mib\":" << MiBFromKb(peak_.memFreeKb)
            << ",\"minimum_mem_available_mib\":" << MiBFromKb(peak_.memAvailableKb)
            << ",\"end_mem_total_mib\":" << MiBFromKb(end_.memTotalKb)
            << ",\"end_mem_free_mib\":" << MiBFromKb(end_.memFreeKb)
            << ",\"end_mem_available_mib\":" << MiBFromKb(end_.memAvailableKb)
            << "},\"reason\":\"" << JsonEscape(reason_) << "\"}";
        return out.str();
    }

private:
    void Sample() {
        MemorySample sample = ReadMemorySample(target_);
        if (!sample.valid) {
            if (reason_.empty()) reason_ = sample.reason;
            return;
        }
        last_ = sample;
        if (sample.rss > peak_.rss) peak_.rss = sample.rss;
        if (sample.vmSize > peak_.vmSize) peak_.vmSize = sample.vmSize;
        if (sample.vmPeak > peak_.vmPeak) peak_.vmPeak = sample.vmPeak;
        if (sample.pss > peak_.pss) peak_.pss = sample.pss;
        if (sample.privateDirty > peak_.privateDirty) peak_.privateDirty = sample.privateDirty;
        if (sample.anonymous > peak_.anonymous) peak_.anonymous = sample.anonymous;
        if (sample.cgroupCurrent > peak_.cgroupCurrent) peak_.cgroupCurrent = sample.cgroupCurrent;
        if (sample.cgroupPeak > peak_.cgroupPeak) peak_.cgroupPeak = sample.cgroupPeak;
        if (sample.memTotalKb > peak_.memTotalKb) peak_.memTotalKb = sample.memTotalKb;
        if (sample.memFreeKb >= 0 && (peak_.memFreeKb < 0 || sample.memFreeKb < peak_.memFreeKb))
            peak_.memFreeKb = sample.memFreeKb;
        if (sample.memAvailableKb >= 0 && (peak_.memAvailableKb < 0 || sample.memAvailableKb < peak_.memAvailableKb))
            peak_.memAvailableKb = sample.memAvailableKb;
        if (sample.buffersKb > peak_.buffersKb) peak_.buffersKb = sample.buffersKb;
        if (sample.cachedKb > peak_.cachedKb) peak_.cachedKb = sample.cachedKb;
        if (sample.swapTotalKb > peak_.swapTotalKb) peak_.swapTotalKb = sample.swapTotalKb;
        if (sample.swapFreeKb >= 0 && (peak_.swapFreeKb < 0 || sample.swapFreeKb < peak_.swapFreeKb))
            peak_.swapFreeKb = sample.swapFreeKb;
        sumRss_ += sample.rss;
        ++sampleCount_;
    }

    bool enabled_ = false;
    int sampleMs_ = 10;
    bool started_ = false;
    std::atomic<bool> running_{false};
    std::thread sampler_;
    MemoryTarget target_;
    MemorySample baseline_;
    MemorySample peak_;
    MemorySample last_;
    MemorySample end_;
    int64_t sumRss_ = 0;
    int sampleCount_ = 0;
    std::string reason_;
};

// Single-use rendezvous: every client thread arrives once - whether or not
// its own connection succeeded, since a failed client still needs to
// "arrive" so this gate isn't stuck waiting on a party that will never show
// up - then the owning thread starts memory sampling once all of them are
// waiting, and releases everyone together right before the timed loop
// begins.
class StartGate {
public:
    explicit StartGate(unsigned expected) : expected_(expected) {}

    void ArriveAndWait() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++arrived_;
        cv_.notify_all();
        cv_.wait(lock, [this] { return released_; });
    }

    void WaitUntilReady() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return arrived_ == expected_; });
    }

    void Release() {
        std::lock_guard<std::mutex> lock(mutex_);
        released_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    unsigned expected_;
    unsigned arrived_ = 0;
    bool released_ = false;
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
        {"setuint64", "{\"value\":18446744073709551615}", "scalar", 0, false, "", ""},
        {"getuint64", "{}", "scalar", 0, true, "", ""},
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

static ClientResult RunOneClient(const Config& cfg, const TestCase& test, Barrier* barrier,
                                 StartGate* startGate, MemoryMeasurement* memory,
                                 bool startMemoryHere) {
    ClientResult result;

    std::unique_ptr<WsClient> ws;
    std::unique_ptr<HttpClient> http;
    bool oneShot = (cfg.transport == "http" && cfg.http_mode == "oneshot");

    if (cfg.transport == "ws") {
        ws.reset(new WsClient());
        if (!ws->Connect(cfg.host, cfg.port, cfg.timeout_s)) {
            std::cerr << "[es1client] WS connect failed: " << ws->LastError() << "\n";
            // Barrier still needs Abort() to avoid deadlocking the
            // survivors' per-round waits below, but StartGate is a one-time
            // rendezvous before that loop even starts - this thread just
            // arrives immediately with nothing to contribute, so the other
            // clients' memory measurement isn't held hostage by one bad
            // connection.
            if (barrier) barrier->Abort();
            if (startGate) startGate->ArriveAndWait();
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

    if (startGate) {
        startGate->ArriveAndWait();
    } else if (startMemoryHere && memory) {
        memory->Begin();
    }

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
    MemoryMeasurement memory(cfg.memory_measure, cfg.memory_sample_ms);
    if (clientCount <= 1) {
        results[0] = RunOneClient(cfg, test, nullptr, nullptr, &memory, true);
    } else {
        Barrier barrier(static_cast<unsigned>(clientCount));
        StartGate startGate(static_cast<unsigned>(clientCount));
        std::vector<std::thread> threads;
        for (int c = 0; c < clientCount; ++c) {
            threads.emplace_back([&, c] {
                results[c] = RunOneClient(cfg, test, &barrier, &startGate, &memory, false);
            });
        }
        startGate.WaitUntilReady();
        memory.Begin();
        startGate.Release();
        for (auto& t : threads) t.join();
    }
    memory.End();

    // Aggregate: one round-trip sample per round = max across clients for that round
    // (the round only completes once the slowest client finishes), matching the
    // Python matrix runner's aggregate_results_by_round.
    std::vector<double> aggregated;
    int totalSkipped = 0;
    int completedClients = 0;
    size_t rounds = cfg.iterations;
    for (auto& r : results) {
        totalSkipped += r.skipped;
        if (r.skipped == 0) ++completedClients;
    }
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
        << "\"skipped\":" << stats.skipped
        << ",\"expected_clients\":" << clientCount
        << ",\"completed_clients\":" << completedClients
        << ",\"memory\":" << memory.ToJson();

    if (measureUs >= 0) {
        out << ",\"measure_us\":" << static_cast<long long>(measureUs);
    }
    out << "}";

    EmitLine(out.str());
}

// ===========================================================================
// Cold-start gate: wait until the ES1Benchmark JSON-RPC method is ready
// ===========================================================================

static bool IsBenchmarkReady(const Config& cfg) {
    std::string request = BuildRequest("getuint64", "{}");
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

    if (cfg.result_file_enabled) {
        g_resultFile.open(cfg.result_file, std::ios::out | std::ios::trunc);
        if (!g_resultFile.is_open()) {
            std::cerr << "[es1client] WARNING: could not open result file '" << cfg.result_file
                      << "' - continuing with stdout only.\n";
        }
    }

    if (cfg.mode == "coldstart") {
        double bootToReady = 0;
        std::cerr << "[es1client] Waiting for ES1Benchmark at " << cfg.host << ":" << cfg.port << " ...\n";
        if (!WaitForThunderReady(cfg, &bootToReady)) {
            std::cerr << "[es1client] ES1Benchmark never became ready within "
                      << cfg.coldstart_timeout_s << "s - aborting.\n";
            return 1;
        }
        std::ostringstream readyLine;
        readyLine << "{\"event\":\"coldstart_ready\",\"boot_to_ready_seconds\":" << bootToReady << "}";
        EmitLine(readyLine.str());
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
