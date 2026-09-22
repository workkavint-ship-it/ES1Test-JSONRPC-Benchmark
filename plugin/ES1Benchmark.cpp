/*
 * If not stated otherwise in this file or this component's LICENSE file the
 * following copyright and licenses apply:
 *
 * Copyright 2024 Metrological
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include "ES1Benchmark.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace WPEFramework {
namespace Plugin {

    namespace {
        static Metadata<ES1Benchmark> metadata(
            1, 0, 0,
            {},
            {},
            {}
        );

        inline uint64_t GetUnixMicroseconds() {
            return std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        }
    }

    const string ES1Benchmark::Initialize(PluginHost::IShell* service) {
        _service = service;
        _service->AddRef();

        _implementation = _service->Root<Exchange::IES1Benchmark>(
            _connectionId, 2000, _T("ES1BenchmarkImplementation"));
        if (_implementation == nullptr) {
            _service->Release();
            _service = nullptr;
            return _T("ES1Benchmark implementation could not be instantiated");
        }

        Exchange::JES1Benchmark::Register(*this, _implementation);

        Config config;
        config.FromString(service->ConfigLine());
        const string notifyHost = config.NotifyHost.Value();
        const uint16_t notifyPort = config.NotifyPort.Value();
        const string thunderHost = config.ThunderHost.Value();
        const uint16_t thunderPort = config.ThunderPort.Value();

        std::thread([notifyHost, notifyPort, thunderHost, thunderPort] {
            const uint64_t bootUs = GetUnixMicroseconds();
            char request[512];
            ::snprintf(request, sizeof(request),
                "GET /trigger?host=%s&port=%u&boot_time_us=%llu HTTP/1.0\r\n"
                "Host: %s\r\n\r\n",
                thunderHost.c_str(),
                static_cast<unsigned>(thunderPort),
                static_cast<unsigned long long>(bootUs),
                notifyHost.c_str());

            const int socketFd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (socketFd < 0) return;

            timeval timeout { 2, 0 };
            ::setsockopt(socketFd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
            ::setsockopt(socketFd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

            sockaddr_in address {};
            address.sin_family = AF_INET;
            address.sin_port = ::htons(notifyPort);
            address.sin_addr.s_addr = ::inet_addr(notifyHost.c_str());
            if (::connect(socketFd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0) {
                ::send(socketFd, request, ::strlen(request), 0);
            }
            ::close(socketFd);
        }).detach();

        return string();
    }

    void ES1Benchmark::Deinitialize(PluginHost::IShell* /* service */) {
        Exchange::JES1Benchmark::Unregister(*this);
        if (_implementation != nullptr) {
            _implementation->Release();
            _implementation = nullptr;
        }
        if (_service != nullptr) {
            _service->Release();
            _service = nullptr;
        }
        _connectionId = 0;
    }

    string ES1Benchmark::Information() const {
        return string("ES1 JSON-RPC round-trip benchmark plugin facade");
    }

} // namespace Plugin
} // namespace WPEFramework
