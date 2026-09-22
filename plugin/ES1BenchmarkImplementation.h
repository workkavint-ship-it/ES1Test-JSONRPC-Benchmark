/*
 * If not stated otherwise in this file or this component's LICENSE file the
 * following copyright and licenses apply:
 *
 * Copyright 2024 Metrological
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#pragma once

#include "Module.h"
#include <interfaces/IES1Benchmark.h>

namespace WPEFramework {
namespace Plugin {

    class ES1BenchmarkImplementation : public Exchange::IES1Benchmark {
    public:
        ES1BenchmarkImplementation() = default;
        ~ES1BenchmarkImplementation() override = default;

        ES1BenchmarkImplementation(const ES1BenchmarkImplementation&) = delete;
        ES1BenchmarkImplementation& operator=(const ES1BenchmarkImplementation&) = delete;

        BEGIN_INTERFACE_MAP(ES1BenchmarkImplementation)
            INTERFACE_ENTRY(Exchange::IES1Benchmark)
        END_INTERFACE_MAP

        uint32_t SetString(const string& value) override;
        uint32_t GetString(const uint32_t size, string& value) override;

        uint32_t SetArray(const std::vector<uint8_t>& value) override;
        uint32_t GetArray(const uint32_t size, std::vector<uint8_t>& value) override;

        uint32_t SetMixedArray(const std::vector<Exchange::IES1Benchmark::MixedElement>& value) override;
        uint32_t GetMixedArray(const uint32_t count, std::vector<Exchange::IES1Benchmark::MixedElement>& value) override;

        uint32_t SetNestedObjects(const std::vector<Exchange::IES1Benchmark::NestedObject>& value) override;
        uint32_t GetNestedObjects(const uint32_t count, std::vector<Exchange::IES1Benchmark::NestedObject>& value) override;

        uint32_t SetUint32(const uint32_t value) override;
        uint32_t GetUint32(uint32_t& value) override;

        uint32_t SetUint64(const uint64_t value) override;
        uint32_t GetUint64(uint64_t& value) override;

        uint32_t SetBool(const bool value) override;
        uint32_t GetBool(bool& value) override;

        uint32_t SetFloat(const float value) override;
        uint32_t GetFloat(float& value) override;

        uint32_t SetDouble(const double value) override;
        uint32_t GetDouble(double& value) override;

        uint32_t MeasureCopyCost(const uint32_t size, uint64_t& us) override;
        uint32_t MeasureStringResizeCost(const uint32_t size, uint64_t& us) override;
        uint32_t MeasureMixedAssignCost(const uint32_t count, uint64_t& us) override;
        uint32_t MeasureNestedAssignCost(const uint32_t count, uint64_t& us) override;
    };

} // namespace Plugin
} // namespace WPEFramework
