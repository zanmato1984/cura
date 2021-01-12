#pragma once

#include "cura/common/types.h"

#include <memory>

#ifdef USE_CUDF
namespace rmm::mr {
class device_memory_resource;
}
#else
namespace arrow {
class MemoryPool;
}
#endif

namespace cura::driver {
struct Option;
} // namespace cura::driver

namespace cura::execution {

using cura::driver::Option;

struct MemoryResource {
  enum class Mode : int8_t {
    ARENA = 0,
    ARENA_PER_THREAD,
    POOL,
    POOL_PER_THREAD,
    MANAGED,
    CUDA,
  };

  virtual ~MemoryResource() = default;

#ifdef USE_CUDF
  using Underlying = rmm::mr::device_memory_resource;
#else
  using Underlying = arrow::MemoryPool;
#endif

  virtual Underlying *preConcatenate(ThreadId thread_id) const = 0;
  virtual Underlying *concatenate() const = 0;
  virtual Underlying *converge() const = 0;

protected:
  virtual void allocatePreConcatenate() = 0;
  virtual void reclaimPreConcatenate() = 0;
  virtual void allocateConcatenate() = 0;
  virtual void reclaimConcatenate() = 0;
  virtual void allocateConverge() = 0;
  virtual void reclaimConverge() = 0;

  friend struct Executor;
};

std::unique_ptr<MemoryResource> createMemoryResource(const Option &option);

} // namespace cura::execution
