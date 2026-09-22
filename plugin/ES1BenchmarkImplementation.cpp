#include "ES1BenchmarkImplementation.h"

#include <array>
#include <chrono>
#include <cstring>
#include <utility>

namespace WPEFramework {
namespace Plugin {

    namespace {
        template <typename T, std::size_t... Is>
        constexpr std::array<T, sizeof...(Is)> fill_array(T value, std::index_sequence<Is...>) {
            return {{ (static_cast<void>(Is), value)... }};
        }

        template <typename T, std::size_t N>
        constexpr std::array<T, N> fill_array(T value) {
            return fill_array<T>(value, std::make_index_sequence<N>{});
        }

        static constexpr auto s_staticCharBuffer = fill_array<char, 4 * 1024 * 1024>('A');
        static constexpr auto s_staticByteBuffer = fill_array<uint8_t, 256 * 1024>(uint8_t{0xAA});
        static uint32_t s_staticUint32 = 0xFFFFFFFFu;
        static uint64_t s_staticUint64 = 0xFFFFFFFFFFFFFFFFull;
        static bool s_staticBool = true;
        static float s_staticFloat = 3.4028235e+38f;
        static double s_staticDouble = 1.7976931348623157e+308;

        static const std::vector<Exchange::IES1Benchmark::MixedElement> s_staticMixedVec = [] {
            std::vector<Exchange::IES1Benchmark::MixedElement> values;
            values.reserve(4228);
            for (uint32_t index = 0; index < 4228; ++index) {
                values.push_back({index, "item" + std::to_string(index), index * 3.14159, (index % 2 == 0)});
            }
            return values;
        }();

        static const std::vector<Exchange::IES1Benchmark::NestedObject> s_staticNestedVec = [] {
            std::vector<Exchange::IES1Benchmark::NestedObject> values;
            values.reserve(1736);
            for (uint32_t index = 0; index < 1736; ++index) {
                Exchange::IES1Benchmark::NestedObject object;
                object.id = index;
                object.flag = (index % 2 == 0);
                object.score = index * 3.14159;
                object.data.label = "item" + std::to_string(index);
                object.data.nested.count = index;
                object.data.nested.inner.value = index * 3;
                object.data.nested.inner.name = "deep" + std::to_string(index);
                values.push_back(object);
            }
            return values;
        }();
    }

    SERVICE_REGISTRATION(ES1BenchmarkImplementation, 1, 0);

    uint32_t ES1BenchmarkImplementation::SetString(const string& /* value */) { return Core::ERROR_NONE; }
    uint32_t ES1BenchmarkImplementation::SetArray(const std::vector<uint8_t>& /* value */) { return Core::ERROR_NONE; }
    uint32_t ES1BenchmarkImplementation::SetMixedArray(const std::vector<Exchange::IES1Benchmark::MixedElement>& /* value */) { return Core::ERROR_NONE; }
    uint32_t ES1BenchmarkImplementation::SetNestedObjects(const std::vector<Exchange::IES1Benchmark::NestedObject>& /* value */) { return Core::ERROR_NONE; }
    uint32_t ES1BenchmarkImplementation::SetUint32(const uint32_t /* value */) { return Core::ERROR_NONE; }
    uint32_t ES1BenchmarkImplementation::SetUint64(const uint64_t /* value */) { return Core::ERROR_NONE; }
    uint32_t ES1BenchmarkImplementation::SetBool(const bool /* value */) { return Core::ERROR_NONE; }
    uint32_t ES1BenchmarkImplementation::SetFloat(const float /* value */) { return Core::ERROR_NONE; }
    uint32_t ES1BenchmarkImplementation::SetDouble(const double /* value */) { return Core::ERROR_NONE; }

    uint32_t ES1BenchmarkImplementation::GetString(const uint32_t size, string& value) {
        value.resize(size);
        std::memcpy(&value[0], s_staticCharBuffer.data(), size);
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::GetArray(const uint32_t size, std::vector<uint8_t>& value) {
        value.resize(size);
        std::memcpy(value.data(), s_staticByteBuffer.data(), size);
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::GetMixedArray(const uint32_t count, std::vector<Exchange::IES1Benchmark::MixedElement>& value) {
        value.assign(s_staticMixedVec.begin(), s_staticMixedVec.begin() + count);
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::GetNestedObjects(const uint32_t count, std::vector<Exchange::IES1Benchmark::NestedObject>& value) {
        value.assign(s_staticNestedVec.begin(), s_staticNestedVec.begin() + count);
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::GetUint32(uint32_t& value) {
        std::memcpy(&value, &s_staticUint32, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::GetUint64(uint64_t& value) {
        std::memcpy(&value, &s_staticUint64, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::GetBool(bool& value) {
        std::memcpy(&value, &s_staticBool, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::GetFloat(float& value) {
        std::memcpy(&value, &s_staticFloat, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::GetDouble(double& value) {
        std::memcpy(&value, &s_staticDouble, sizeof(value));
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::MeasureCopyCost(const uint32_t size, uint64_t& us) {
        std::vector<uint8_t> destination;
        const auto start = std::chrono::steady_clock::now();
        destination.resize(size);
        std::memcpy(destination.data(), s_staticByteBuffer.data(), size);
        const auto end = std::chrono::steady_clock::now();
        us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::MeasureStringResizeCost(const uint32_t size, uint64_t& us) {
        string destination;
        const auto start = std::chrono::steady_clock::now();
        destination.resize(size);
        std::memcpy(&destination[0], s_staticCharBuffer.data(), size);
        const auto end = std::chrono::steady_clock::now();
        us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::MeasureMixedAssignCost(const uint32_t count, uint64_t& us) {
        std::vector<Exchange::IES1Benchmark::MixedElement> destination;
        const auto start = std::chrono::steady_clock::now();
        destination.assign(s_staticMixedVec.begin(), s_staticMixedVec.begin() + count);
        const auto end = std::chrono::steady_clock::now();
        us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        return Core::ERROR_NONE;
    }

    uint32_t ES1BenchmarkImplementation::MeasureNestedAssignCost(const uint32_t count, uint64_t& us) {
        std::vector<Exchange::IES1Benchmark::NestedObject> destination;
        const auto start = std::chrono::steady_clock::now();
        destination.assign(s_staticNestedVec.begin(), s_staticNestedVec.begin() + count);
        const auto end = std::chrono::steady_clock::now();
        us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        return Core::ERROR_NONE;
    }

} // namespace Plugin
} // namespace WPEFramework
