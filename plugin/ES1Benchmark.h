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

#pragma once

#include "Module.h"
#include <interfaces/IES1Benchmark.h>
#include <interfaces/json/JES1Benchmark.h>

namespace WPEFramework {
namespace Plugin {

    class ES1Benchmark : public PluginHost::IPlugin
                       , public PluginHost::JSONRPC {
    public:
        ES1Benchmark(const ES1Benchmark&) = delete;
        ES1Benchmark& operator=(const ES1Benchmark&) = delete;

        ES1Benchmark() = default;
        ~ES1Benchmark() override = default;

        // Plugin configuration — read from ES1Benchmark.json on device
        class Config : public Core::JSON::Container {
        public:
            Config()
                : Core::JSON::Container()
                , NotifyHost("127.0.0.1")  // IP of the Python coldstart server
                , NotifyPort(8080)          // Port of the Python coldstart server
                , ThunderHost("127.0.0.1") // IP of this device (sent to Python so it can connect back)
                , ThunderPort(55555)        // Thunder JSON-RPC port on this device
            {
                Add(_T("notifyhost"),  &NotifyHost);
                Add(_T("notifyport"),  &NotifyPort);
                Add(_T("thunderhost"), &ThunderHost);
                Add(_T("thunderport"), &ThunderPort);
            }
            Core::JSON::String  NotifyHost;
            Core::JSON::DecUInt16 NotifyPort;
            Core::JSON::String  ThunderHost;
            Core::JSON::DecUInt16 ThunderPort;
        };

        BEGIN_INTERFACE_MAP(ES1Benchmark)
            INTERFACE_ENTRY(PluginHost::IPlugin)
            INTERFACE_ENTRY(PluginHost::IDispatcher)
            INTERFACE_AGGREGATE(Exchange::IES1Benchmark, _implementation)
        END_INTERFACE_MAP

        // IPlugin
        const string Initialize(PluginHost::IShell* service) override;
        void Deinitialize(PluginHost::IShell* service) override;
        string Information() const override;

    private:
        PluginHost::IShell* _service { nullptr };
        uint32_t _connectionId { 0 };
        Exchange::IES1Benchmark* _implementation { nullptr };
    };

} // namespace Plugin
} // namespace WPEFramework
