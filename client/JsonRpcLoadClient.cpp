/*
 * Standalone configurable ES1Benchmark load client.
 * The existing es1client target is intentionally separate and unchanged.
 */

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

struct Config {
    std::string mode = "warm";
    std::string transport = "ws";
    std::string httpMode = "session";
    std::string scope = "scalar";
    std::string operation = "both";
    std::string tier = "single";
    std::string size = "5KB";
    int clients = 100;
    int iterations = 10;
    int warmup = 0;
    std::string host = "127.0.0.1";
    int port = 9998;
    int timeoutS = 30;
    int memorySampleMs = 10;
    bool memoryMeasure = true;
    bool resultFileEnabled = true;
    std::string resultFile = "/opt/JsonRpcLoadClient-result.jsonl";
};

struct TestCase {
    std::string method;
    std::string tier;
    std::string params;
    bool isGet;
    long targetBytes;
};

static std::ofstream resultFile;
static std::mutex outputMutex;
static std::atomic<long long> requestId{1};

static std::string Trim(const std::string& value) {
    const size_t first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    const size_t last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

static bool IsTrue(const std::string& value) {
    return value == "true" || value == "1" || value == "yes";
}

static bool LoadConfig(const std::string& path, Config* config) {
    std::ifstream input(path);
    if (!input.is_open()) return false;
    std::string line;
    while (std::getline(input, line)) {
        const std::string item = Trim(line);
        if (item.empty() || item[0] == '#') continue;
        const size_t separator = item.find('=');
        if (separator == std::string::npos) continue;
        const std::string key = Trim(item.substr(0, separator));
        const std::string value = Trim(item.substr(separator + 1));
        if (key == "mode") config->mode = value;
        else if (key == "transport") config->transport = value;
        else if (key == "http_mode") config->httpMode = value;
        else if (key == "test_scope") config->scope = value;
        else if (key == "operation") config->operation = value;
        else if (key == "tier") config->tier = value;
        else if (key == "size") config->size = value;
        else if (key == "clients") config->clients = std::stoi(value);
        else if (key == "iterations") config->iterations = std::stoi(value);
        else if (key == "warmup") config->warmup = std::stoi(value);
        else if (key == "host") config->host = value;
        else if (key == "port") config->port = std::stoi(value);
        else if (key == "timeout_s") config->timeoutS = std::stoi(value);
        else if (key == "memory_measure") config->memoryMeasure = IsTrue(value);
        else if (key == "memory_sample_ms") config->memorySampleMs = std::max(1, std::stoi(value));
        else if (key == "result_file_enabled") config->resultFileEnabled = IsTrue(value);
        else if (key == "result_file") config->resultFile = value;
    }
    return config->clients > 0 && config->iterations > 0 && config->warmup >= 0;
}

static long ParseSize(std::string value) {
    for (char& character : value) character = static_cast<char>(std::toupper(static_cast<unsigned char>(character)));
    if (value.size() > 2 && value.substr(value.size() - 2) == "KB") return std::stol(value.substr(0, value.size() - 2)) * 1024L;
    if (value.size() > 2 && value.substr(value.size() - 2) == "MB") return std::stol(value.substr(0, value.size() - 2)) * 1024L * 1024L;
    if (!value.empty() && value.back() == 'B') return std::stol(value.substr(0, value.size() - 1));
    return std::stol(value);
}

static std::string JsonEscape(const std::string& value) {
    std::string escaped;
    for (char character : value) {
        if (character == '\\' || character == '"') escaped += '\\';
        escaped += character;
    }
    return escaped;
}

static void Emit(const std::string& line) {
    std::lock_guard<std::mutex> lock(outputMutex);
    std::cout << line << std::endl;
    if (resultFile.is_open()) {
        resultFile << line << std::endl;
        resultFile.flush();
    }
}

static int ConnectTcp(const Config& config, std::string* error) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        *error = "socket";
        return -1;
    }
    timeval timeout{config.timeoutS, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    int enabled = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(config.port));
    if (::inet_pton(AF_INET, config.host.c_str(), &address.sin_addr) != 1 ||
        ::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        *error = "connect";
        ::close(fd);
        return -1;
    }
    return fd;
}

static bool SendAll(int fd, const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        const ssize_t count = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (count <= 0) return false;
        sent += static_cast<size_t>(count);
    }
    return true;
}

static bool RecvExact(int fd, char* output, size_t length) {
    size_t received = 0;
    while (received < length) {
        const ssize_t count = ::recv(fd, output + received, length - received, 0);
        if (count <= 0) return false;
        received += static_cast<size_t>(count);
    }
    return true;
}

class WsClient {
public:
    explicit WsClient(const Config& config) : config_(config) {}
    ~WsClient() { Close(); }

    bool Open(std::string* error) {
        fd_ = ConnectTcp(config_, error);
        if (fd_ < 0) return false;
        const std::string key = "dGhlIHNhbXBsZSBub25jZQ==";
        std::ostringstream request;
        request << "GET /jsonrpc HTTP/1.1\r\nHost: " << config_.host << ":" << config_.port
                << "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                << "Sec-WebSocket-Key: " << key << "\r\nSec-WebSocket-Version: 13\r\n\r\n";
        if (!SendAll(fd_, request.str())) {
            *error = "websocket handshake send";
            return false;
        }
        std::string headers;
        char buffer[512];
        while (headers.find("\r\n\r\n") == std::string::npos) {
            const ssize_t count = ::recv(fd_, buffer, sizeof(buffer), 0);
            if (count <= 0) {
                *error = "websocket handshake receive";
                return false;
            }
            headers.append(buffer, static_cast<size_t>(count));
            if (headers.size() > 16384) {
                *error = "websocket handshake too large";
                return false;
            }
        }
        if (headers.find(" 101 ") == std::string::npos && headers.find(" 101\r") == std::string::npos) {
            *error = "websocket upgrade rejected";
            return false;
        }
        return true;
    }

    bool Call(const std::string& request, std::string* response, std::string* error) {
        std::string frame;
        frame.push_back(static_cast<char>(0x81));
        const uint64_t length = request.size();
        if (length < 126) {
            frame.push_back(static_cast<char>(0x80 | length));
        } else if (length <= 0xFFFF) {
            frame.push_back(static_cast<char>(0x80 | 126));
            frame.push_back(static_cast<char>(length >> 8));
            frame.push_back(static_cast<char>(length));
        } else {
            frame.push_back(static_cast<char>(0x80 | 127));
            for (int shift = 56; shift >= 0; shift -= 8) frame.push_back(static_cast<char>(length >> shift));
        }
        const uint8_t mask[4] = {0x11, 0x22, 0x33, 0x44};
        frame.append(reinterpret_cast<const char*>(mask), sizeof(mask));
        for (size_t index = 0; index < request.size(); ++index) frame.push_back(request[index] ^ mask[index % 4]);
        if (!SendAll(fd_, frame)) {
            *error = "websocket send";
            return false;
        }
        for (;;) {
            uint8_t header[2];
            if (!RecvExact(fd_, reinterpret_cast<char*>(header), sizeof(header))) {
                *error = "websocket receive timeout or close";
                return false;
            }
            const bool finalFrame = (header[0] & 0x80) != 0;
            const uint8_t opcode = header[0] & 0x0F;
            uint64_t payloadLength = header[1] & 0x7F;
            if (payloadLength == 126) {
                uint8_t extended[2];
                if (!RecvExact(fd_, reinterpret_cast<char*>(extended), sizeof(extended))) return false;
                payloadLength = (static_cast<uint64_t>(extended[0]) << 8) | extended[1];
            } else if (payloadLength == 127) {
                uint8_t extended[8];
                if (!RecvExact(fd_, reinterpret_cast<char*>(extended), sizeof(extended))) return false;
                payloadLength = 0;
                for (uint8_t byte : extended) payloadLength = (payloadLength << 8) | byte;
            }
            std::string payload(payloadLength, '\0');
            if (payloadLength > 0 && !RecvExact(fd_, &payload[0], payload.size())) return false;
            if (opcode == 0x9) continue;
            if (opcode == 0x8) {
                *error = "websocket close";
                return false;
            }
            response->append(payload);
            if (finalFrame) return true;
        }
    }

private:
    void Close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
    const Config& config_;
    int fd_ = -1;
};

class HttpClient {
public:
    explicit HttpClient(const Config& config) : config_(config) {}
    ~HttpClient() { Close(); }

    bool Call(const std::string& request, std::string* response, std::string* error) {
        std::string connectError;
        const bool oneShot = config_.httpMode == "oneshot";
        if (oneShot || fd_ < 0) {
            Close();
            fd_ = ConnectTcp(config_, &connectError);
            if (fd_ < 0) {
                *error = connectError;
                return false;
            }
        }
        std::ostringstream message;
        message << "POST /jsonrpc HTTP/1.1\r\nHost: " << config_.host << ":" << config_.port
                << "\r\nContent-Type: application/json\r\nContent-Length: " << request.size()
                << "\r\nConnection: " << (oneShot ? "close" : "keep-alive") << "\r\n\r\n" << request;
        if (!SendAll(fd_, message.str())) {
            *error = "http send";
            Close();
            return false;
        }
        std::string data;
        char buffer[4096];
        size_t headerEnd = std::string::npos;
        long contentLength = -1;
        bool chunked = false;
        while (true) {
            const ssize_t count = ::recv(fd_, buffer, sizeof(buffer), 0);
            if (count <= 0) {
                if (count == 0 && headerEnd != std::string::npos && contentLength < 0 && !chunked) {
                    *response = data.substr(headerEnd + 4);
                    break;
                }
                *error = "http receive timeout or close";
                Close();
                return false;
            }
            data.append(buffer, static_cast<size_t>(count));
            if (headerEnd == std::string::npos) {
                headerEnd = data.find("\r\n\r\n");
                if (headerEnd != std::string::npos) {
                    const std::string headers = data.substr(0, headerEnd);
                    const std::string lower = Lower(headers);
                    chunked = lower.find("transfer-encoding: chunked") != std::string::npos;
                    const size_t position = lower.find("content-length:");
                    if (position != std::string::npos) {
                        const size_t lineEnd = lower.find("\r\n", position);
                        contentLength = std::stol(Trim(headers.substr(position + 15, lineEnd - position - 15)));
                    }
                }
            }
            if (headerEnd != std::string::npos) {
                const std::string body = data.substr(headerEnd + 4);
                if (contentLength >= 0 && static_cast<long>(body.size()) >= contentLength) {
                    *response = body.substr(0, static_cast<size_t>(contentLength));
                    break;
                }
                if (chunked && DecodeChunked(body, response)) break;
            }
        }
        if (oneShot) Close();
        return true;
    }

private:
    static std::string Lower(std::string value) {
        for (char& character : value) character = static_cast<char>(std::tolower(static_cast<unsigned char>(character)));
        return value;
    }

    static bool DecodeChunked(const std::string& input, std::string* output) {
        size_t cursor = 0;
        std::string decoded;
        while (true) {
            const size_t lineEnd = input.find("\r\n", cursor);
            if (lineEnd == std::string::npos) return false;
            const std::string sizeText = Trim(input.substr(cursor, lineEnd - cursor));
            char* end = nullptr;
            const unsigned long size = std::strtoul(sizeText.c_str(), &end, 16);
            if (end == sizeText.c_str()) return false;
            cursor = lineEnd + 2;
            if (input.size() < cursor + size + 2) return false;
            decoded.append(input, cursor, size);
            cursor += size + 2;
            if (size == 0) {
                *output = decoded;
                return true;
            }
        }
    }

    void Close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }
    const Config& config_;
    int fd_ = -1;
};

static std::string BuildRequest(const TestCase& test) {
    std::ostringstream output;
    output << "{\"jsonrpc\":\"2.0\",\"id\":" << requestId.fetch_add(1)
           << ",\"method\":\"ES1Benchmark.1." << test.method << "\",\"params\":" << test.params << "}";
    return output.str();
}

static bool IsSuccess(const std::string& response) {
    return response.find("\"result\":") != std::string::npos && response.find("\"error\":") == std::string::npos;
}

static std::string MakePayload(long size) {
    return std::string(static_cast<size_t>(size), 'A');
}

static std::string BuildArray(long size, long* count) {
    const long elements = std::max<long>(1, size / 4);
    if (count) *count = elements;
    std::ostringstream output;
    output << "[";
    for (long index = 0; index < elements; ++index) {
        if (index) output << ",";
        output << (index % 256);
    }
    output << "]";
    return output.str();
}

static std::string BuildMixedArray(long size, long* count) {
    const long elements = std::min<long>(4228, std::max<long>(1, size / 62));
    if (count) *count = elements;
    std::ostringstream output;
    output << "[";
    for (long index = 0; index < elements; ++index) {
        if (index) output << ",";
        output << "{\"id\":" << index << ",\"name\":\"item" << index
               << "\",\"value\":3.14159,\"flag\":" << (index % 2 == 0 ? "true" : "false") << "}";
    }
    output << "]";
    return output.str();
}

static std::string BuildNestedArray(long size, long* count) {
    const long elements = std::min<long>(1736, std::max<long>(1, size / 151));
    if (count) *count = elements;
    std::ostringstream output;
    output << "[";
    for (long index = 0; index < elements; ++index) {
        if (index) output << ",";
        output << "{\"id\":" << index << ",\"flag\":true,\"score\":3.14,"
               << "\"data\":{\"label\":\"item\",\"nested\":{\"count\":" << index
               << ",\"inner\":{\"value\":3,\"name\":\"deep\"}}}}";
    }
    output << "]";
    return output.str();
}

static std::vector<TestCase> BuildTests(const Config& config) {
    const std::vector<std::pair<std::string, long>> tiers = {
        {"500B", 500}, {"5KB", 5 * 1024}, {"50KB", 50 * 1024}, {"150KB", 150 * 1024},
        {"500KB", 500 * 1024}, {"1MB", 1024 * 1024}, {"4MB", 4 * 1024 * 1024}
    };
    std::vector<std::pair<std::string, long>> selected;
    if (config.tier == "multiple") selected = tiers;
    else selected.push_back({config.size, ParseSize(config.size)});
    std::vector<TestCase> tests;
    const bool allowSet = config.operation == "set" || config.operation == "both";
    const bool allowGet = config.operation == "get" || config.operation == "both";
    if (config.scope == "scalar" || config.scope == "both") {
        if (allowSet) tests.push_back({"setuint64", "scalar", "{\"value\":18446744073709551615}", false, 0});
        if (allowGet) tests.push_back({"getuint64", "scalar", "{}", true, 0});
    }
    if (config.scope == "tier" || config.scope == "both") {
        for (const auto& tier : selected) {
            if (allowSet) tests.push_back({"setstring", tier.first, "{\"value\":\"" + MakePayload(tier.second) + "\"}", false, tier.second});
            if (allowGet) tests.push_back({"getstring", tier.first, "{\"size\":" + std::to_string(tier.second) + "}", true, tier.second});
            if (tier.second <= 256 * 1024) {
                long count = 0;
                const std::string array = BuildArray(tier.second, &count);
                if (allowSet) tests.push_back({"setarray", tier.first, "{\"value\":" + array + "}", false, tier.second});
                if (allowGet) tests.push_back({"getarray", tier.first, "{\"size\":" + std::to_string(count) + "}", true, tier.second});

                const std::string mixed = BuildMixedArray(tier.second, &count);
                if (allowSet) tests.push_back({"setmixedarray", tier.first, "{\"value\":" + mixed + "}", false, tier.second});
                if (allowGet) tests.push_back({"getmixedarray", tier.first, "{\"count\":" + std::to_string(count) + "}", true, tier.second});

                const std::string nested = BuildNestedArray(tier.second, &count);
                if (allowSet) tests.push_back({"setnestedobjects", tier.first, "{\"value\":" + nested + "}", false, tier.second});
                if (allowGet) tests.push_back({"getnestedobjects", tier.first, "{\"count\":" + std::to_string(count) + "}", true, tier.second});
            }
        }
    }
    return tests;
}

static int ReadWpeRssKb() {
    DIR* directory = ::opendir("/proc");
    if (!directory) return -1;
    int result = -1;
    dirent* entry = nullptr;
    while ((entry = ::readdir(directory)) != nullptr) {
        if (entry->d_type != DT_DIR || !std::isdigit(static_cast<unsigned char>(entry->d_name[0]))) continue;
        std::ifstream name(std::string("/proc/") + entry->d_name + "/comm");
        std::string process;
        std::getline(name, process);
        if (process.find("WPEFramework") == std::string::npos) continue;
        std::ifstream status(std::string("/proc/") + entry->d_name + "/status");
        std::string line;
        while (std::getline(status, line)) {
            if (line.rfind("VmRSS:", 0) == 0) {
                std::istringstream value(line.substr(7));
                value >> result;
                break;
            }
        }
        if (result >= 0) break;
    }
    ::closedir(directory);
    return result;
}

struct RunStats {
    int success = 0;
    int failed = 0;
    double minMs = 0;
    double maxMs = 0;
    double averageMs = 0;
    int baselineRssKb = -1;
    int peakRssKb = -1;
};

static RunStats RunTest(const Config& config, const TestCase& test) {
    std::vector<double> latencies;
    std::mutex statsMutex;
    std::atomic<int> success{0};
    std::atomic<int> failed{0};
    std::atomic<int> baseline{ReadWpeRssKb()};
    std::atomic<int> peak{baseline.load()};
    std::atomic<bool> running{true};
    std::thread sampler;
    if (config.memoryMeasure) {
        sampler = std::thread([&] {
            while (running) {
                const int rss = ReadWpeRssKb();
                int current = peak.load();
                while (rss >= 0 && rss > current && !peak.compare_exchange_weak(current, rss)) {}
                std::this_thread::sleep_for(std::chrono::milliseconds(config.memorySampleMs));
            }
        });
    }
    auto client = [&] {
        std::unique_ptr<WsClient> ws;
        std::unique_ptr<HttpClient> http;
        std::string error;
        if (config.transport == "ws") {
            ws.reset(new WsClient(config));
            if (!ws->Open(&error)) {
                failed.fetch_add(config.iterations);
                return;
            }
        } else {
            http.reset(new HttpClient(config));
        }
        const std::string request = BuildRequest(test);
        auto call = [&](std::string* response) {
            error.clear();
            return ws ? ws->Call(request, response, &error) : http->Call(request, response, &error);
        };
        for (int warmup = 0; warmup < config.warmup; ++warmup) {
            std::string response;
            call(&response);
        }
        for (int iteration = 0; iteration < config.iterations; ++iteration) {
            const auto start = std::chrono::steady_clock::now();
            std::string response;
            const bool ok = call(&response) && IsSuccess(response);
            const auto end = std::chrono::steady_clock::now();
            if (!ok) {
                ++failed;
            } else {
                ++success;
                std::lock_guard<std::mutex> lock(statsMutex);
                latencies.push_back(std::chrono::duration<double, std::milli>(end - start).count());
            }
        }
    };
    std::vector<std::thread> clients;
    clients.reserve(static_cast<size_t>(config.clients));
    for (int index = 0; index < config.clients; ++index) clients.emplace_back(client);
    for (auto& thread : clients) thread.join();
    running = false;
    if (sampler.joinable()) sampler.join();
    RunStats stats;
    stats.success = success.load();
    stats.failed = failed.load();
    stats.baselineRssKb = baseline.load();
    stats.peakRssKb = peak.load();
    if (!latencies.empty()) {
        stats.minMs = *std::min_element(latencies.begin(), latencies.end());
        stats.maxMs = *std::max_element(latencies.begin(), latencies.end());
        double total = 0;
        for (double latency : latencies) total += latency;
        stats.averageMs = total / latencies.size();
    }
    return stats;
}

int main(int argc, char** argv) {
    const std::string path = argc > 1 ? argv[1] : "/opt/JsonRpcLoadClient.config";
    Config config;
    if (!LoadConfig(path, &config)) {
        std::cerr << "[JsonRpcLoadClient] invalid or missing config: " << path << "\n";
        return 1;
    }
    if (config.resultFileEnabled) resultFile.open(config.resultFile, std::ios::out | std::ios::trunc);
    const std::vector<TestCase> tests = BuildTests(config);
    if (tests.empty()) {
        std::cerr << "[JsonRpcLoadClient] no tests selected\n";
        return 1;
    }
    for (const TestCase& test : tests) {
        const RunStats stats = RunTest(config, test);
        const int expected = config.clients * config.iterations;
        std::ostringstream output;
        output << "{\"ts\":\"load\",\"transport\":\"" << JsonEscape(config.transport)
               << "\",\"http_mode\":\"" << JsonEscape(config.httpMode)
               << "\",\"method\":\"" << test.method << "\",\"tier\":\"" << test.tier
               << "\",\"clients\":" << config.clients << ",\"iterations\":" << config.iterations
               << ",\"warmup\":" << config.warmup << ",\"expected_requests\":" << expected
               << ",\"successful_requests\":" << stats.success << ",\"failed_requests\":" << stats.failed
               << ",\"roundtrip_ms\":{\"min\":" << stats.minMs << ",\"max\":" << stats.maxMs
               << ",\"avg\":" << stats.averageMs << "},\"rss_baseline_mib\":"
               << (stats.baselineRssKb < 0 ? "null" : std::to_string(stats.baselineRssKb / 1024.0))
               << ",\"rss_peak_mib\":"
               << (stats.peakRssKb < 0 ? "null" : std::to_string(stats.peakRssKb / 1024.0))
               << ",\"rss_peak_delta_mib\":"
               << (stats.baselineRssKb < 0 || stats.peakRssKb < 0 ? "null" : std::to_string((stats.peakRssKb - stats.baselineRssKb) / 1024.0))
               << "}";
        Emit(output.str());
    }
    return 0;
}
