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

#include "ES1Benchmark.h"
#include <arpa/inet.h>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>

namespace WPEFramework {
namespace Plugin {

    namespace {
        static Metadata<ES1Benchmark> metadata(
            // Version (Major, Minor, Patch)
            1, 0, 0,
            // Preconditions
            {},
            // Terminations
            {},
            // Controls
            {}
        );

        // Returns Unix epoch time in microseconds — same epoch as Python time.time()
        inline uint64_t GetUnixMicroseconds() {
            return std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();
        }

        // Compile-time array fill: expands an index_sequence of N indices,
        // discards each index, substitutes 'value' for every one. The whole
        // array is computed during translation and placed directly in the
        // binary's read-only data - no runtime fill code executes at all.
        template <typename T, std::size_t... Is>
        constexpr std::array<T, sizeof...(Is)> fill_array(T value, std::index_sequence<Is...>) {
            return {{ (static_cast<void>(Is), value)... }};
        }
        template <typename T, std::size_t N>
        constexpr std::array<T, N> fill_array(T value) {
            return fill_array<T>(value, std::make_index_sequence<N>{});
        }

        // Static benchmark payload data. Computed entirely at compile time (see
        // fill_array above), read-only for the plugin's lifetime, so concurrent
        // Get* calls from multiple clients can read it without locking.
        // Content is arbitrary - only size/shape matters here.
        static constexpr auto s_staticCharBuffer = fill_array<char, 4 * 1024 * 1024>('A');       // GetString source, up to @restrict:0..4M
        static constexpr auto s_staticByteBuffer = fill_array<uint8_t, 256 * 1024>(uint8_t{0xAA}); // GetArray source, up to @restrict:0..256K

        static uint32_t s_staticUint32 = 0xFFFFFFFFu;
        static uint64_t s_staticUint64 = 0xFFFFFFFFFFFFFFFFull;
        static bool     s_staticBool   = true;
        static float    s_staticFloat  = 3.4028235e+38f;
        static double   s_staticDouble = 1.7976931348623157e+308;

        // Namespace-scope statics, built once via IIFE at static-init time (before any
        // handler can ever run) - referenced directly by name in the Get* handlers, no
        // function call and no lazy-init guard check on every access.
        static const std::vector<Exchange::IES1Benchmark::MixedElement> s_staticMixedVec = [] {
            std::vector<Exchange::IES1Benchmark::MixedElement> v;
            v.reserve(4228);
            for (uint32_t i = 0; i < 4228; ++i) {
                v.push_back({ i, "item" + std::to_string(i), i * 3.14159, (i % 2 == 0) });
            }
            return v;
        }();

        static const std::vector<Exchange::IES1Benchmark::NestedObject> s_staticNestedVec = [] {
            std::vector<Exchange::IES1Benchmark::NestedObject> v;
            v.reserve(1736);
            for (uint32_t i = 0; i < 1736; ++i) {
                Exchange::IES1Benchmark::NestedObject obj;
                obj.id = i;
                obj.flag = (i % 2 == 0);
                obj.score = i * 3.14159;
                obj.data.label = "item" + std::to_string(i);
                obj.data.nested.count = i;
                obj.data.nested.inner.value = i * 3;
                obj.data.nested.inner.name = "deep" + std::to_string(i);
                v.push_back(obj);
            }
            return v;
        }();

    }

    const string ES1Benchmark::Initialize(PluginHost::IShell* service)
    {
        Exchange::JES1Benchmark::Register(*this, this);

        // Read configuration from plugin JSON (e.g. /etc/WPEFramework/plugins/ES1Benchmark.json)
        Config config;
        config.FromString(service->ConfigLine());

        const string notifyHost  = config.NotifyHost.Value();
        const uint16_t notifyPort  = config.NotifyPort.Value();
        const string thunderHost = config.ThunderHost.Value();
        const uint16_t thunderPort = config.ThunderPort.Value();

        // Fire-and-forget HTTP GET to the Python coldstart monitor.
        // The monitor receives this, then connects back to thunderHost:thunderPort
        // to run the full benchmark suite.
        // Times out silently after 2 seconds if no monitor is running.
        std::thread([notifyHost, notifyPort, thunderHost, thunderPort] {
            uint64_t boot_us = GetUnixMicroseconds();
            char req[512];
            ::snprintf(req, sizeof(req),
                "GET /trigger?host=%s&port=%u&boot_time_us=%llu HTTP/1.0\r\n"
                "Host: %s\r\n\r\n",
                thunderHost.c_str(),
                static_cast<unsigned>(thunderPort),
                static_cast<unsigned long long>(boot_us),
                notifyHost.c_str());

            int sock = ::socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) return;

            struct timeval tv { 2, 0 };
            ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

            struct sockaddr_in addr {};
            addr.sin_family      = AF_INET;
            addr.sin_port        = ::htons(notifyPort);
            addr.sin_addr.s_addr = ::inet_addr(notifyHost.c_str());

            if (::connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
                ::send(sock, req, ::strlen(req), 0);
            }
            ::close(sock);
        }).detach();

        return string();
    }

    void ES1Benchmark::Deinitialize(PluginHost::IShell* /* service */)
    {
        Exchange::JES1Benchmark::Unregister(*this);
    }

    string ES1Benchmark::Information() const
    {
        return string("ES1 JSON-RPC round-trip benchmark echo plugin");
    }

    // Set* handlers: no work needed - the framework's own deserialize step is what's
    // being measured. Nothing is stored, so concurrent Set* calls from different
    // clients never interfere with each other.

    uint32_t ES1Benchmark::SetString(const string& /* value */)
    {
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::SetArray(const std::vector<uint8_t>& /* value */)
    {
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::SetMixedArray(const std::vector<Exchange::IES1Benchmark::MixedElement>& /* value */)
    {
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::SetNestedObjects(const std::vector<Exchange::IES1Benchmark::NestedObject>& /* value */)
    {
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::SetUint32(const uint32_t /* value */)
    {
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::SetUint64(const uint64_t /* value */)
    {
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::SetBool(const bool /* value */)
    {
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::SetFloat(const float /* value */)
    {
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::SetDouble(const double /* value */)
    {
        return Core::ERROR_NONE;
    }

    // Get* handlers: populate the output from pre-built static data. 'size'/'count'
    // are bounded by @restrict on the interface, so the framework rejects
    // out-of-range requests before these ever run.

    uint32_t ES1Benchmark::GetString(const uint32_t size, string& value)
    {
        value.resize(size);
        std::memcpy(&value[0], s_staticCharBuffer.data(), size);
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::GetArray(const uint32_t size, std::vector<uint8_t>& value)
    {
        value.resize(size);
        std::memcpy(value.data(), s_staticByteBuffer.data(), size);
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::GetMixedArray(const uint32_t count, std::vector<Exchange::IES1Benchmark::MixedElement>& value)
    {
        value.assign(s_staticMixedVec.begin(), s_staticMixedVec.begin() + count);
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::GetNestedObjects(const uint32_t count, std::vector<Exchange::IES1Benchmark::NestedObject>& value)
    {
        value.assign(s_staticNestedVec.begin(), s_staticNestedVec.begin() + count);
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::GetUint32(uint32_t& value)
    {
        std::memcpy(&value, &s_staticUint32, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::GetUint64(uint64_t& value)
    {
        std::memcpy(&value, &s_staticUint64, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::GetBool(bool& value)
    {
        std::memcpy(&value, &s_staticBool, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::GetFloat(float& value)
    {
        std::memcpy(&value, &s_staticFloat, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1Benchmark::GetDouble(double& value)
    {
        std::memcpy(&value, &s_staticDouble, sizeof(value));
        return Core::ERROR_NONE;
    }

    // Calibration only - mirrors GetArray exactly: resize() is included in the
    // timed window, since GetArray's vector must be sized before it can be
    // memcpy'd into (same reasoning as MeasureStringResizeCost/GetString).
    uint32_t ES1Benchmark::MeasureCopyCost(const uint32_t size, uint64_t& us)
    {
        std::vector<uint8_t> dst;

        auto t0 = std::chrono::steady_clock::now();
        dst.resize(size);
        std::memcpy(dst.data(), s_staticByteBuffer.data(), size);
        auto t1 = std::chrono::steady_clock::now();

        us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        return Core::ERROR_NONE;
    }

    // Calibration only - mirrors GetString exactly: resize() is included in the
    // timed window (same reasoning as MeasureCopyCost/GetArray above, which also
    // resize() a std::vector before the memcpy, now that GetArray uses
    // std::vector<uint8_t> instead of the old raw-array @length mechanism).
    uint32_t ES1Benchmark::MeasureStringResizeCost(const uint32_t size, uint64_t& us)
    {
        string dst;

        auto t0 = std::chrono::steady_clock::now();
        dst.resize(size);
        std::memcpy(&dst[0], s_staticCharBuffer.data(), size);
        auto t1 = std::chrono::steady_clock::now();

        us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        return Core::ERROR_NONE;
    }

    // Calibration only - mirrors exactly what GetMixedArray does internally (assign
    // onto a freshly default-constructed vector, no pre-reserve), so this is a
    // faithful stand-in for that handler's real cost, not just a similar-looking one.
    uint32_t ES1Benchmark::MeasureMixedAssignCost(const uint32_t count, uint64_t& us)
    {
        std::vector<Exchange::IES1Benchmark::MixedElement> dst;

        auto t0 = std::chrono::steady_clock::now();
        dst.assign(s_staticMixedVec.begin(), s_staticMixedVec.begin() + count);
        auto t1 = std::chrono::steady_clock::now();

        us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        return Core::ERROR_NONE;
    }

    // Calibration only - mirrors GetNestedObjects the same way.
    uint32_t ES1Benchmark::MeasureNestedAssignCost(const uint32_t count, uint64_t& us)
    {
        std::vector<Exchange::IES1Benchmark::NestedObject> dst;

        auto t0 = std::chrono::steady_clock::now();
        dst.assign(s_staticNestedVec.begin(), s_staticNestedVec.begin() + count);
        auto t1 = std::chrono::steady_clock::now();

        us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        return Core::ERROR_NONE;
    }

} // namespace Plugin
} // namespace WPEFramework
