#pragma once

#include "cura/kernel/kernel.h"
#include "cura/type/data_type.h"

#include <mutex>

namespace cura::kernel {

using cura::type::Schema;

struct Union : public HeapNonStreamKernel {
  explicit Union(KernelId id, Schema schema_)
      : HeapNonStreamKernel(id), schema(std::move(schema_)) {}

  std::string name() const override { return "Union"; }

  void push(const Context &ctx, ThreadId thread_id, KernelId upstream,
            std::shared_ptr<const Fragment> fragment) const override;

  void concatenate(const Context &ctx) const override;

  void converge(const Context &ctx) const override;

  std::shared_ptr<const Fragment> heapFragment() const override {
    return converged_fragment;
  }

private:
  Schema schema;

private:
  mutable std::mutex push_mutex;
  mutable std::vector<std::shared_ptr<const Fragment>> pushed_fragments;
  mutable std::shared_ptr<const Fragment> concatenated_fragment;
  mutable std::shared_ptr<const Fragment> converged_fragment;
};

struct UnionAll : public NonSourceStreamKernel {
  explicit UnionAll(KernelId id) : NonSourceStreamKernel(id) {}

  std::string name() const override { return "UnionAll"; }

protected:
  std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment) const override;
};

} // namespace cura::kernel
