#pragma once

#include <cstddef>
#include <cstdint>

namespace cura {

using PipelineId = int64_t;
using ThreadId = int64_t;
using KernelId = int64_t;
using SourceId = int64_t;

constexpr ThreadId VoidThreadId = -1;
constexpr KernelId VoidKernelId = -1;

} // namespace cura
