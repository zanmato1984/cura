#pragma once

#include "cura/expression/expressions.h"
#include "cura/kernel/kernel.h"
#include "cura/relational/rels.h"

#include <mutex>
#include <numeric>

namespace cura::kernel {

using cura::expression::ColumnIdx;
using cura::relational::BuildSide;
using cura::relational::JoinType;
using cura::type::Schema;

/// HashJoinBuild/Probe always come in pair, so they don't follow the rule of
/// passes data through heap. They share the data via an implicit way that both
/// hold a shared pointer to cudf::hash_join.

namespace detail {

struct HashJoinImpl;

} // namespace detail

struct HashJoinBuild : NonStreamKernel {
  HashJoinBuild(KernelId id, Schema schema_, std::vector<ColumnIdx> keys_);

  void push(const Context &ctx, ThreadId thread_id, KernelId upstream,
            std::shared_ptr<const Fragment> fragment) const override;

  void concatenate(const Context &ctx) const override;

  void converge(const Context &ctx) const override;

  std::string name() const override { return "HashJoinBuild"; }

  std::string toString() const override {
    return Kernel::toString() + "(keys: [" +
           std::accumulate(keys.begin() + 1, keys.end(),
                           std::to_string(keys[0]),
                           [](const auto &all, const auto &key) {
                             return all + ", " + std::to_string(key);
                           }) +
           "])";
  }

  std::shared_ptr<const detail::HashJoinImpl> hashJoinImpl() const {
    return impl;
  }

private:
  Schema schema;
  std::vector<ColumnIdx> keys;

  std::shared_ptr<detail::HashJoinImpl> impl;

private:
  mutable std::mutex push_mutex;
  mutable std::vector<std::shared_ptr<const Fragment>> pushed_fragments;
  mutable std::shared_ptr<const Fragment> concatenated_fragment;
  mutable std::shared_ptr<const Fragment> converged_fragment;
};

struct HashJoinProbe : NonSourceStreamKernel {
public:
  HashJoinProbe(KernelId id, Schema schema_, JoinType join_type_,
                std::vector<ColumnIdx> keys_,
                std::shared_ptr<const HashJoinBuild> hash_join_build,
                BuildSide build_side_)
      : NonSourceStreamKernel(id), schema(std::move(schema_)),
        build_kernel_id(hash_join_build->id), join_type(join_type_),
        keys(std::move(keys_)), build_side(build_side_),
        impl(hash_join_build->hashJoinImpl()) {}

  std::string name() const override { return "HashJoinProbe"; }

  std::string toString() const override;

protected:
  std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment) const override;

private:
  Schema schema;

  JoinType join_type;
  std::vector<ColumnIdx> keys;
  KernelId build_kernel_id;
  BuildSide build_side;

  std::shared_ptr<const detail::HashJoinImpl> impl;
};

} // namespace cura::kernel
