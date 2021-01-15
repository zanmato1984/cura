#include "cura/kernel/aggregate.h"
#include "aggregate_helper.h"
#include "cura/expression/aggregation.h"
#include "helper.h"

#include <sstream>

namespace cura::kernel {

using cura::expression::aggregationOperatorToString;

Aggregate::Aggregate(KernelId id, Schema input_schema_, Schema output_schema_,
                     std::vector<ColumnIdx> keys_,
                     std::vector<PhysicalAggregation> aggregations_)
    : HeapNonStreamKernel(id), input_schema(std::move(input_schema_)),
      output_schema(std::move(output_schema_)), keys(std::move(keys_)),
      aggregations(std::move(aggregations_)) {
  CURA_ASSERT(!aggregations.empty(), "Empty aggregations for Aggregate");
}

void Aggregate::push(const Context &ctx, ThreadId thread_id, KernelId upstream,
                     std::shared_ptr<const Fragment> fragment) const {
  std::lock_guard<std::mutex> lock(push_mutex);
  pushed_fragments.emplace_back(fragment);
}

void Aggregate::concatenate(const Context &ctx) const {
  if (pushed_fragments.empty()) {
    return;
  }

  auto fragments = std::move(pushed_fragments);
  concatenated_fragment = detail::concatFragments(
      ctx.memory_resource->concatenate(), input_schema, fragments);
}

void Aggregate::converge(const Context &ctx) const {
  if (!concatenated_fragment) {
    return;
  }

  auto concatenated = std::move(concatenated_fragment);

  converged_fragment =
      detail::doAggregate(ctx, output_schema, keys, aggregations, concatenated);
}

std::string Aggregate::toString() const {
  std::stringstream ss;
  if (!keys.empty()) {
    ss << "keys: ["
       << std::accumulate(keys.begin() + 1, keys.end(), std::to_string(keys[0]),
                          [](const auto &all, const auto &key) {
                            return all + ", " + std::to_string(key);
                          }) +
              "], ";
  }
  ss << "aggregations: ["
     << std::accumulate(aggregations.begin() + 1, aggregations.end(),
                        aggregationOperatorToString(aggregations[0].op) + "(" +
                            std::to_string(aggregations[0].idx) + ")",
                        [](const auto &all, const auto &aggregation) {
                          return all + ", " +
                                 aggregationOperatorToString(aggregation.op) +
                                 "(" + std::to_string(aggregation.idx) + ")";
                        }) +
            "]";
  return Kernel::toString() + "(" + ss.str() + ")";
}

} // namespace cura::kernel
