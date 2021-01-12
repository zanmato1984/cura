#pragma once

#include <cstddef>
#include <cstdint>

namespace cura::driver {

struct Option {
  Option();

  int8_t memory_resource;
  bool exclusive_default_memory_resource;
  size_t memory_resource_size;
  size_t threads_per_pipeline;
  size_t memory_resource_size_per_thread;

  bool bucket_aggregate;
  size_t bucket_aggregate_buckets;
};

} // namespace cura::driver
