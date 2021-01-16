#pragma once

#include "cura/kernel/aggregate.h"

namespace cura::kernel {

struct BucketAggregate : HeapNonStreamKernel {
  BucketAggregate(KernelId id, Schema input_schema_, Schema output_schema_,
                  std::vector<ColumnIdx> keys_,
                  std::vector<PhysicalAggregation> aggregations_,
                  size_t buckets_);

  void push(const Context &ctx, ThreadId thread_id, KernelId upstream,
            std::shared_ptr<const Fragment> fragment) const override;

  void concatenate(const Context &ctx) const override;

  void converge(const Context &ctx) const override;

  std::string name() const override { return "BucketAggregate"; }

  std::string toString() const override;

  std::shared_ptr<const Fragment> heapFragment() const override {
    return converged_fragment;
  }

private:
  Schema input_schema;
  Schema output_schema;
  std::vector<ColumnIdx> keys;
  std::vector<PhysicalAggregation> aggregations;
  size_t buckets;

private:
  mutable std::mutex push_mutex;
  mutable std::vector<std::shared_ptr<const Fragment>> pushed_fragments;
#ifdef USE_CUDF
  mutable std::vector<std::vector<std::shared_ptr<const Fragment>>>
      fragment_buckets;
#else
  mutable std::shared_ptr<const Fragment> concatenated_fragment;
#endif
  mutable std::shared_ptr<const Fragment> converged_fragment;
};

} // namespace cura::kernel
