#include "cura/execution/memory_resource.h"
#include "cura/common/errors.h"
#include "cura/driver/option.h"

#include <string>

#include <arrow/memory_pool.h>

namespace cura::execution {

namespace detail {

template <typename T>
struct LightWeightMemoryResource : public MemoryResource {
  explicit LightWeightMemoryResource(const Option &option)
  {
    l = arrow::MemoryPool::CreateDefault();
  }

  ~LightWeightMemoryResource() {
  }

  Underlying *preConcatenate(ThreadId thread_id) const override {
    return l.get();
  }

  Underlying *concatenate() const override { return l.get(); }

  Underlying *converge() const override { return l.get(); }

protected:
  void allocatePreConcatenate() override {}

  void reclaimPreConcatenate() override {}

  void allocateConcatenate() override {}

  void reclaimConcatenate() override {}

  void allocateConverge() override {}

  void reclaimConverge() override {}

private:
  std::unique_ptr<T> l;
};

template <typename T>
struct LightWeightPerThreadMemoryResource : public MemoryResource {
  LightWeightPerThreadMemoryResource(const Option &option)
      :
        thread_ls(option.threads_per_pipeline) {
    CURA_ASSERT(option.memory_resource_size,
                "LightWeightPerThreadMemoryResource size must not be zero");
    CURA_ASSERT(
        option.threads_per_pipeline,
        "LightWeightPerThreadMemoryResource thread number must not be zero");
    CURA_ASSERT(
        option.memory_resource_size_per_thread,
        "LightWeightPerThreadMemoryResource size per thread must not be zero");
    p = std::make_unique<PT>();
    l = std::make_unique<LT<PT>>(p.get(), option.memory_resource_size,
                                 option.memory_resource_size);
    CURA_ASSERT(l, "LightWeightPerThreadMemoryResource allocation failed");

    if (option.exclusive_default_memory_resource) {
      orig_default = rmm::mr::set_current_device_resource(l.get());
    }

    for (size_t i = 0; i < option.threads_per_pipeline; i++) {
      thread_ps[i] = std::make_unique<PT>();
      thread_ls[i] = std::make_unique<LT<PT>>(
          thread_ps[i].get(), option.memory_resource_size_per_thread,
          option.memory_resource_size_per_thread);
      CURA_ASSERT(thread_ls[i], "LightWeightPerThreadMemoryResource "
                                "per thread allocation failed");
    }
  }

  ~LightWeightPerThreadMemoryResource() {
  }

  Underlying *preConcatenate(ThreadId thread_id) const override {
    CURA_ASSERT(thread_id < thread_ls.size(),
                "LightWeightPerThreadMemoryResource invalid thread ID " +
                    std::to_string(thread_id) + " (" +
                    std::to_string(thread_ls.size()) + " threads in total)");
    return thread_ls[thread_id].get();
  }

  Underlying *concatenate() const override { return l.get(); }

  Underlying *converge() const override { return l.get(); }

protected:
  void allocatePreConcatenate() override {}

  void reclaimPreConcatenate() override {}

  void allocateConcatenate() override {}

  void reclaimConcatenate() override {}

  void allocateConverge() override {}

  void reclaimConverge() override {}

private:
  std::unique_ptr<T> l;
  std::vector<std::unique_ptr<T>> thread_ls;
};

template <typename T> struct PrimitiveMemoryResource : public MemoryResource {
  explicit PrimitiveMemoryResource(const Option &option) {
    mr = arrow::MemoryPool::CreateDefault();
  }

  Underlying *preConcatenate(ThreadId thread_id) const override {
    return mr.get();
  }

  Underlying *concatenate() const override { return mr.get(); }

  Underlying *converge() const override { return mr.get(); }

protected:
  void allocatePreConcatenate() override {}

  void reclaimPreConcatenate() override {}

  void allocateConcatenate() override {}

  void reclaimConcatenate() override {}

  void allocateConverge() override {}

  void reclaimConverge() override {}

private:
  std::unique_ptr<T> mr;
};

using ArenaMemoryResource = LightWeightMemoryResource<arrow::MemoryPool>;
using ArenaPerThreadMemoryResource =
    LightWeightPerThreadMemoryResource<arrow::MemoryPool>;
using PoolMemoryResource = LightWeightMemoryResource<arrow::MemoryPool>;
using PoolPerThreadMemoryResource =
    LightWeightPerThreadMemoryResource<arrow::MemoryPool>;
using ManagedMemoryResource = PrimitiveMemoryResource<arrow::MemoryPool>;
using CudaMemoryResource = PrimitiveMemoryResource<arrow::MemoryPool>;

} // namespace detail

std::unique_ptr<MemoryResource> createMemoryResource(const Option &option) {
  switch (static_cast<MemoryResource::Mode>(option.memory_resource)) {
  case MemoryResource::Mode::ARENA:
    return std::make_unique<detail::ArenaMemoryResource>(option);
  case MemoryResource::Mode::ARENA_PER_THREAD:
    return std::make_unique<detail::ArenaPerThreadMemoryResource>(option);
  case MemoryResource::Mode::POOL:
    return std::make_unique<detail::PoolMemoryResource>(option);
  case MemoryResource::Mode::POOL_PER_THREAD:
    return std::make_unique<detail::PoolPerThreadMemoryResource>(option);
  case MemoryResource::Mode::MANAGED:
    return std::make_unique<detail::ManagedMemoryResource>(option);
  case MemoryResource::Mode::CUDA:
    return std::make_unique<detail::CudaMemoryResource>(option);
  default:
    CURA_FAIL("Unsupported memory resource type: " +
              std::to_string(option.memory_resource));
  }
}

} // namespace cura::execution
