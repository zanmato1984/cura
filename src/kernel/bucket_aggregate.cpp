#include "cura/kernel/bucket_aggregate.h"
#include "aggregate_helper.h"
#include "cura/data/column_scalar.h"
#include "cura/data/column_vector.h"
#include "cura/data/fragment.h"
#include "cura/expression/aggregation.h"
#include "helper.h"

#include <sstream>

#ifdef USE_CUDF
#include <cudf/partitioning.hpp>
#endif

namespace cura::kernel {

#ifdef USE_CUDF
using cura::data::Column;
using cura::data::ColumnVector;
using cura::data::ColumnVectorCudfColumn;
using cura::data::ColumnVectorCudfColumnView;
using cura::data::createCudfColumnVector;
#endif

using cura::expression::aggregationOperatorToString;

BucketAggregate::BucketAggregate(KernelId id, Schema input_schema_,
                                 Schema output_schema_,
                                 std::vector<ColumnIdx> keys_,
                                 std::vector<PhysicalAggregation> aggregations_,
                                 size_t buckets_)
    : HeapNonStreamKernel(id), input_schema(std::move(input_schema_)),
      output_schema(std::move(output_schema_)), keys(std::move(keys_)),
      aggregations(std::move(aggregations_)), buckets(buckets_)
#ifdef USE_CUDF
      ,
      fragment_buckets(buckets_)
#endif
{
  CURA_ASSERT(!aggregations.empty(), "Empty aggregations for BucketAggregate");
}

void BucketAggregate::push(const Context &ctx, ThreadId thread_id,
                           KernelId upstream,
                           std::shared_ptr<const Fragment> fragment) const {
#ifdef USE_CUDF
  std::vector<cudf::size_type> partition_keys(keys.size());
  std::transform(keys.begin(), keys.end(), partition_keys.begin(),
                 [](const auto &key) { return key; });
  auto [partitioned, offsets] = cudf::hash_partition(
      fragment->cudf(), partition_keys, static_cast<int>(buckets),
      cudf::hash_id::HASH_MURMUR3, rmm::cuda_stream_default,
      ctx.memory_resource->preConcatenate(thread_id));
  auto partitioned_frag =
      std::make_shared<Fragment>(input_schema, std::move(partitioned));
  {
    std::lock_guard<std::mutex> lock(push_mutex);
    pushed_fragments.emplace_back(partitioned_frag);
  }
  for (size_t i = 0; i < buckets; i++) {
    size_t size = (i == buckets - 1 ? partitioned_frag->size() - offsets[i]
                                    : offsets[i + 1] - offsets[i]);
    std::vector<std::shared_ptr<const Column>> bucket_columns(
        partitioned_frag->numColumns());
    std::transform(partitioned_frag->begin(), partitioned_frag->end(),
                   bucket_columns.begin(),
                   [&, offsets = std::ref(offsets)](const auto &column) {
                     auto cv =
                         std::dynamic_pointer_cast<const ColumnVector>(column);
                     return cv->view(offsets.get()[i], size);
                   });
    auto bucket_frag = std::make_shared<Fragment>(std::move(bucket_columns));
    std::lock_guard<std::mutex> lock(push_mutex);
    fragment_buckets[i].emplace_back(bucket_frag);
  }
#else
  std::lock_guard<std::mutex> lock(push_mutex);
  pushed_fragments.emplace_back(fragment);
#endif
}

void BucketAggregate::concatenate(const Context &ctx) const {
#ifndef USE_CUDF
  if (pushed_fragments.empty()) {
    return;
  }

  auto fragments = std::move(pushed_fragments);
  concatenated_fragment = detail::concatFragments(
      ctx.memory_resource->concatenate(), input_schema, fragments);
#endif
}

void BucketAggregate::converge(const Context &ctx) const {
#ifdef USE_CUDF
  std::vector<std::shared_ptr<const Fragment>> result_buckets(buckets);
  std::transform(fragment_buckets.begin(), fragment_buckets.end(),
                 result_buckets.begin(), [&](const auto &bucket) {
                   auto concatenated_frag = detail::concatFragments(
                       ctx.memory_resource->converge(), input_schema, bucket);
                   return detail::doAggregate(ctx, output_schema, keys,
                                              aggregations, concatenated_frag);
                 });
  converged_fragment = detail::concatFragments(ctx.memory_resource->converge(),
                                               output_schema, result_buckets);
#else
  if (!concatenated_fragment) {
    return;
  }

  auto concatenated = std::move(concatenated_fragment);

  converged_fragment =
      detail::doAggregate(ctx, output_schema, keys, aggregations, concatenated);
#endif
}

std::string BucketAggregate::toString() const {
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
