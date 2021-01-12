#include "cura/driver/option.h"

namespace cura::driver {

/// Defaults.
constexpr int8_t default_memory_resource = 0;
constexpr bool default_exclusive_default_memory_resource = false;
constexpr size_t default_memory_resource_size = 0;
constexpr size_t default_threads_per_pipeline = 0;
constexpr size_t default_memory_resource_size_per_thread = 0;
constexpr bool default_bucket_aggregate = false;
constexpr size_t default_bucket_aggregate_buckets = 0;

/// For testing.
// constexpr int8_t default_memory_resource = *;
// constexpr bool default_exclusive_default_memory_resource = *;
// constexpr size_t default_memory_resource_size = 256 * 1024 * 1024L;
// constexpr size_t default_threads_per_pipeline = 8;
// constexpr size_t default_memory_resource_size_per_thread = 64 * 1024 * 1024L;
// constexpr bool default_bucket_aggregate = false;
// constexpr size_t default_bucket_aggregate_buckets = 8;

Option::Option()
    : memory_resource(default_memory_resource),
      exclusive_default_memory_resource(
          default_exclusive_default_memory_resource),
      memory_resource_size(default_memory_resource_size),
      threads_per_pipeline(default_threads_per_pipeline),
      memory_resource_size_per_thread(default_memory_resource_size_per_thread),
      bucket_aggregate(default_bucket_aggregate),
      bucket_aggregate_buckets(default_bucket_aggregate_buckets) {}

} // namespace cura::driver
