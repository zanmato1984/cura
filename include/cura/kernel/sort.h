#pragma once

#include "cura/expression/expressions.h"
#include "cura/kernel/kernel.h"
#include "cura/relational/rels.h"
#include "cura/type/data_type.h"

#include <mutex>

namespace cura::kernel {

using cura::expression::ColumnIdx;
using cura::type::Schema;

using Order = cura::relational::SortInfo::Order;
using NullOrder = cura::relational::SortInfo::NullOrder;

struct PhysicalSortInfo {
  ColumnIdx idx;
  Order order;
  NullOrder null_order;

  std::string toString() const {
    return std::to_string(idx) + " " +
           (order == Order::ASCENDING ? "ASC" : "DESC") + " NULL_" +
           (null_order == NullOrder::FIRST ? "FIRST" : "LAST");
  }
};

struct Sort : public HeapNonStreamKernel {
  explicit Sort(KernelId id, Schema schema_,
                std::vector<PhysicalSortInfo> sort_infos_)
      : HeapNonStreamKernel(id), schema(std::move(schema_)),
        sort_infos(std::move(sort_infos_)) {}

  std::string name() const override { return "Sort"; }

  void push(const Context &ctx, ThreadId thread_id, KernelId upstream,
            std::shared_ptr<const Fragment> fragment) const override;

  void concatenate(const Context &ctx) const override;

  void converge(const Context &ctx) const override;

  std::string toString() const override {
    return Kernel::toString() + "(" +
           std::accumulate(sort_infos.begin() + 1, sort_infos.end(),
                           sort_infos[0].toString(),
                           [](const auto &all, const auto &sort_info) {
                             return all + ", " + sort_info.toString();
                           }) +
           ")";
  }

  std::shared_ptr<const Fragment> heapFragment() const override {
    return converged_fragment;
  }

private:
  Schema schema;
  std::vector<PhysicalSortInfo> sort_infos;

private:
  mutable std::mutex push_mutex;
  mutable std::vector<std::shared_ptr<const Fragment>> pushed_fragments;
  mutable std::shared_ptr<const Fragment> concatenated_fragment;
  mutable std::shared_ptr<const Fragment> converged_fragment;
};

} // namespace cura::kernel
