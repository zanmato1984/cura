#pragma once

#include "cura/expression/aggregation.h"
#include "cura/kernel/kernel.h"

#include <mutex>

namespace cura::kernel {

using cura::expression::AggregationOperator;
using cura::expression::ColumnIdx;
using cura::type::DataType;
using cura::type::Schema;

struct PhysicalAggregation {
  ColumnIdx idx;
  AggregationOperator op;
  DataType data_type;

  /// Used for NthElement's parameter.
  int64_t n = 0;
};

struct Aggregate : HeapNonStreamKernel {
  Aggregate(KernelId id, Schema input_schema_, Schema output_schema_,
            std::vector<ColumnIdx> keys_,
            std::vector<PhysicalAggregation> aggregations_);

  void push(const Context &ctx, ThreadId thread_id, KernelId upstream,
            std::shared_ptr<const Fragment> fragment) const override;

  void concatenate(const Context &ctx) const override;

  void converge(const Context &ctx) const override;

  std::string name() const override { return "Aggregate"; }

  std::string toString() const override;

  std::shared_ptr<const Fragment> heapFragment() const override {
    return converged_fragment;
  }

private:
  Schema input_schema;
  Schema output_schema;
  std::vector<ColumnIdx> keys;
  std::vector<PhysicalAggregation> aggregations;

private:
  mutable std::mutex push_mutex;
  mutable std::vector<std::shared_ptr<const Fragment>> pushed_fragments;
  mutable std::shared_ptr<const Fragment> concatenated_fragment;
  mutable std::shared_ptr<const Fragment> converged_fragment;
};

} // namespace cura::kernel
