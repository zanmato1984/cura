#include "pipeline_generator.h"
#include "cura/driver/option.h"
#include "cura/execution/pipeline.h"
#include "cura/kernel/aggregate.h"
#include "cura/kernel/bucket_aggregate.h"
#include "cura/kernel/filter.h"
#include "cura/kernel/hash_join.h"
#include "cura/kernel/limit.h"
#include "cura/kernel/project.h"
#include "cura/kernel/sort.h"
#include "cura/kernel/sources.h"
#include "cura/kernel/unions.h"

namespace cura::planning {

using cura::expression::Aggregation;
using cura::expression::ColumnIdx;
using cura::expression::ColumnRef;
using cura::expression::NthElement;
using cura::kernel::Aggregate;
using cura::kernel::BucketAggregate;
using cura::kernel::Filter;
using cura::kernel::HashJoinProbe;
using cura::kernel::HeapNonStreamKernel;
using cura::kernel::HeapSource;
using cura::kernel::InputSource;
using cura::kernel::Limit;
using cura::kernel::NonStreamKernel;
using cura::kernel::NonTerminalKernel;
using cura::kernel::PhysicalAggregation;
using cura::kernel::PhysicalSortInfo;
using cura::kernel::Project;
using cura::kernel::Sort;
using cura::kernel::StreamKernel;
using cura::kernel::Terminal;
using cura::kernel::Union;
using cura::kernel::UnionAll;
using cura::relational::SortInfo;

namespace detail {

PipelineBuilder::PipelineBuilder(
    std::vector<std::shared_ptr<const Source>> sources_,
    std::vector<std::shared_ptr<const NonStreamKernel>> non_streams_)
    : sources(std::move(sources_)), non_streams(std::move(non_streams_)) {}

PipelineBuilder::PipelineBuilder(std::vector<PipelineBuilder> &&builders) {
  for (auto &&builder : builders) {
    sources.insert(sources.end(),
                   std::make_move_iterator(builder.sources.begin()),
                   std::make_move_iterator(builder.sources.end()));
    non_streams.insert(non_streams.end(),
                       std::make_move_iterator(builder.non_streams.begin()),
                       std::make_move_iterator(builder.non_streams.end()));
  }
}

PipelineChain mergePipelineChains(std::vector<PipelineChain> &pipeline_chains) {
  PipelineChain merged;

  if (pipeline_chains.empty()) {
    return merged;
  }

  while (true) {
    std::vector<PipelineBuilder> merged_closed_pipelines;
    for (auto &pipeline_chain : pipeline_chains) {
      if (pipeline_chain.closed_pipelines.empty()) {
        continue;
      }
      merged_closed_pipelines.emplace_back(
          std::move(pipeline_chain.closed_pipelines.front()));
      pipeline_chain.closed_pipelines.pop_front();
    }
    if (merged_closed_pipelines.empty()) {
      break;
    }
    merged.closed_pipelines.emplace_back(
        PipelineBuilder(std::move(merged_closed_pipelines)));
  }

  {
    std::vector<PipelineBuilder> open_pipelines;
    for (auto &pipeline_chain : pipeline_chains) {
      if (pipeline_chain.open_pipeline.has_value()) {
        open_pipelines.emplace_back(
            std::move(pipeline_chain.open_pipeline.value()));
      }
    }
    if (!open_pipelines.empty()) {
      merged.open_pipeline = PipelineBuilder(std::move(open_pipelines));
    }
  }

  return merged;
}

} // namespace detail

std::list<std::unique_ptr<Pipeline>>
PipelineGenerator::genPipelines(std::shared_ptr<const Rel> rel) && {
  auto kernel = visit(rel);
  auto it = pipeline_chains.find(kernel);
  CURA_ASSERT(it != pipeline_chains.end(),
              "Couldn't find kernel for rel node: " + rel->toString());
  auto &pipeline_chain = it->second;
  std::reverse(pipeline_chain.closed_pipelines.begin(),
               pipeline_chain.closed_pipelines.end());

  std::list<std::unique_ptr<Pipeline>> pipelines;
  for (auto &pipeline : pipeline_chain.closed_pipelines) {
    CURA_ASSERT(!pipeline.non_streams.empty(),
                "Closed pipeline with no non-stream kernel");
    auto terminal = makeKernel<Terminal>(std::move(pipeline.non_streams));
    pipelines.emplace_back(std::make_unique<Pipeline>(
        pipelines.size(), false, std::move(pipeline.sources), terminal));
  }
  pipelines.emplace_back(std::make_unique<Pipeline>(
      pipelines.size(), true, std::move(pipeline_chain.open_pipeline->sources),
      nullptr));
  return pipelines;
}

std::shared_ptr<Kernel> PipelineGenerator::visitInputSource(
    std::shared_ptr<const RelInputSource> input_source,
    std::vector<std::shared_ptr<Kernel>> &children) {
  return combineResult(makeKernel<InputSource>(input_source->sourceId()),
                       children);
}

std::shared_ptr<Kernel>
PipelineGenerator::visitFilter(std::shared_ptr<const RelFilter> filter,
                               std::vector<std::shared_ptr<Kernel>> &children) {
  return combineResult(
      makeKernel<Filter>(filter->output(), filter->condition()), children);
}

std::shared_ptr<Kernel>
PipelineGenerator::visitUnion(std::shared_ptr<const RelUnion> u,
                              std::vector<std::shared_ptr<Kernel>> &children) {
  return combineResult(makeKernel<Union>(u->output()), children);
}

std::shared_ptr<Kernel> PipelineGenerator::visitUnionAll(
    std::shared_ptr<const RelUnionAll> union_all,
    std::vector<std::shared_ptr<Kernel>> &children) {
  return combineResult(makeKernel<UnionAll>(), children);
}

std::shared_ptr<Kernel> PipelineGenerator::visitHashJoin(
    std::shared_ptr<const RelHashJoin> hash_join,
    std::vector<std::shared_ptr<Kernel>> &children) {
  CURA_FAIL("Pipeline generator shouldn't see RelHashJoin node directly");
}

std::shared_ptr<Kernel> PipelineGenerator::visitHashJoinBuild(
    std::shared_ptr<const RelHashJoinBuild> hash_join_build,
    std::vector<std::shared_ptr<Kernel>> &children) {
  std::vector<ColumnIdx> keys;
  for (const auto &key : hash_join_build->buildKeys()) {
    keys.emplace_back(key->columnIdx());
  }
  auto build =
      makeKernel<HashJoinBuild>(hash_join_build->output(), std::move(keys));
  hash_join_build_kernels.emplace(hash_join_build, build);
  return combineResult(build, children);
}

std::shared_ptr<Kernel> PipelineGenerator::visitHashJoinProbe(
    std::shared_ptr<const RelHashJoinProbe> hash_join_probe,
    std::vector<std::shared_ptr<Kernel>> &children) {
  auto it = hash_join_build_kernels.find(hash_join_probe->buildInput());
  CURA_ASSERT(it != hash_join_build_kernels.end(),
              "Corresponding hash join build kernel not found");
  std::vector<ColumnIdx> keys;
  for (const auto &key : hash_join_probe->probeKeys()) {
    keys.emplace_back(key->columnIdx());
  }
  auto probe = makeKernel<HashJoinProbe>(
      hash_join_probe->output(), hash_join_probe->joinType(), std::move(keys),
      it->second, hash_join_probe->buildSide());
  return combineResult(probe, children);
}

std::shared_ptr<Kernel> PipelineGenerator::visitProject(
    std::shared_ptr<const RelProject> project,
    std::vector<std::shared_ptr<Kernel>> &children) {
  auto kernel = makeKernel<Project>(project->expressions());
  return combineResult(kernel, children);
}

std::shared_ptr<Kernel> PipelineGenerator::visitAggregate(
    std::shared_ptr<const RelAggregate> aggregate,
    std::vector<std::shared_ptr<Kernel>> &children) {
  std::vector<ColumnIdx> keys(aggregate->groups().size());
  std::transform(aggregate->groups().begin(), aggregate->groups().end(),
                 keys.begin(), [](const auto &e) {
                   auto col_ref = std::dynamic_pointer_cast<const ColumnRef>(e);
                   CURA_ASSERT(col_ref, "Aggregate group must be ColumnRef");
                   return col_ref->columnIdx();
                 });
  std::vector<PhysicalAggregation> aggregations(
      aggregate->aggregations().size());
  std::transform(
      aggregate->aggregations().begin(), aggregate->aggregations().end(),
      aggregations.begin(), [](const auto &e) {
        auto aggregation = std::dynamic_pointer_cast<const Aggregation>(e);
        CURA_ASSERT(aggregation, "Aggregate aggregation must be Aggregation");
        auto col_ref = std::dynamic_pointer_cast<const ColumnRef>(
            aggregation->operands()[0]);
        CURA_ASSERT(col_ref,
                    "Aggregate aggregation must be applied to ColumnRef");
        int64_t n = 0;
        if (auto nth_element =
                std::dynamic_pointer_cast<const NthElement>(aggregation);
            nth_element) {
          n = nth_element->n();
        }
        return PhysicalAggregation{col_ref->columnIdx(),
                                   aggregation->aggregationOperator(),
                                   aggregation->dataType(), n};
      });
  auto kernel = [&]() -> std::shared_ptr<Kernel> {
    if (option.bucket_aggregate && !keys.empty()) {
      return makeKernel<BucketAggregate>(
          aggregate->inputSchema(), aggregate->output(), std::move(keys),
          std::move(aggregations), option.bucket_aggregate_buckets);
    } else {
      return makeKernel<Aggregate>(aggregate->inputSchema(),
                                   aggregate->output(), std::move(keys),
                                   std::move(aggregations));
    }
  }();
  return combineResult(kernel, children);
}

std::shared_ptr<Kernel>
PipelineGenerator::visitSort(std::shared_ptr<const RelSort> sort,
                             std::vector<std::shared_ptr<Kernel>> &children) {
  std::vector<PhysicalSortInfo> sort_infos(sort->sortInfos().size());
  std::transform(
      sort->sortInfos().begin(), sort->sortInfos().end(), sort_infos.begin(),
      [](const auto &sort_info) {
        auto col_ref =
            std::dynamic_pointer_cast<const ColumnRef>(sort_info.expression);
        CURA_ASSERT(col_ref, "SortInfo expression must be ColumnRef");
        return PhysicalSortInfo{col_ref->columnIdx(), sort_info.order,
                                sort_info.null_order};
      });
  auto kernel = makeKernel<Sort>(sort->output(), sort_infos);
  return combineResult(kernel, children);
}

std::shared_ptr<Kernel>
PipelineGenerator::visitLimit(std::shared_ptr<const RelLimit> limit,
                              std::vector<std::shared_ptr<Kernel>> &children) {
  auto kernel = makeKernel<Limit>(limit->offset(), limit->n());
  return combineResult(kernel, children);
}

std::shared_ptr<Kernel> PipelineGenerator::combineResult(
    std::shared_ptr<Kernel> parent,
    std::vector<std::shared_ptr<Kernel>> &children) {
  CURA_ASSERT(parent, "Empty kernel");

  std::vector<detail::PipelineChain> child_pipeline_chains;

  for (const auto &child : children) {
    CURA_ASSERT(child, "Empty kernel");

    if (auto stream = std::dynamic_pointer_cast<StreamKernel>(child); stream) {
      auto non_terminal = std::dynamic_pointer_cast<NonTerminalKernel>(parent);
      CURA_ASSERT(non_terminal,
                  "Meet terminal kernel during pipeline generation");
      stream->downstream = non_terminal;
    }

    auto it = pipeline_chains.find(child);
    CURA_ASSERT(it != pipeline_chains.end(), "Kernel not found");
    child_pipeline_chains.emplace_back(std::move(it->second));
    pipeline_chains.erase(it);
  }

  auto parent_pipeline_chain =
      detail::mergePipelineChains(child_pipeline_chains);
  auto next_parent = parent;
  if (auto stream = std::dynamic_pointer_cast<StreamKernel>(parent); stream) {
    if (auto input_source = std::dynamic_pointer_cast<InputSource>(stream);
        input_source) {
      if (!parent_pipeline_chain.open_pipeline) {
        parent_pipeline_chain.open_pipeline = detail::PipelineBuilder({}, {});
      }
      parent_pipeline_chain.open_pipeline->sources.emplace_back(input_source);
    }
  } else if (auto non_stream =
                 std::dynamic_pointer_cast<NonStreamKernel>(parent);
             non_stream) {
    CURA_ASSERT(parent_pipeline_chain.open_pipeline,
                "Dangling non-stream kernel");
    CURA_ASSERT(parent_pipeline_chain.open_pipeline.value().non_streams.empty(),
                "Open pipeline contains non-stream kernels");

    parent_pipeline_chain.open_pipeline.value().non_streams.emplace_back(
        non_stream);
    parent_pipeline_chain.closed_pipelines.emplace_front(
        std::move(parent_pipeline_chain.open_pipeline.value()));

    if (auto heap_non_stream_kernel =
            std::dynamic_pointer_cast<HeapNonStreamKernel>(non_stream);
        heap_non_stream_kernel) {
      auto heap_source = makeKernel<HeapSource>(heap_non_stream_kernel);
      parent_pipeline_chain.open_pipeline =
          detail::PipelineBuilder({heap_source}, {});

      next_parent = heap_source;
    }
  } else {
    CURA_FAIL("Neither stream nor non-stream kernel");
  }

  pipeline_chains.emplace(next_parent, std::move(parent_pipeline_chain));

  return next_parent;
}

} // namespace cura::planning
