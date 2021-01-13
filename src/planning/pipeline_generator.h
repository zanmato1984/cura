#pragma once

#include "cura/relational/rel_visitor.h"

#include <list>
#include <optional>
#include <unordered_map>

namespace cura::driver {
struct Option;
} // namespace cura::driver

namespace cura::execution {
class Pipeline;
} // namespace cura::execution

namespace cura::kernel {
struct HashJoinBuild;
struct Kernel;
struct NonStreamKernel;
struct Source;
} // namespace cura::kernel

namespace cura::planning {

using cura::driver::Option;
using cura::execution::Pipeline;
using cura::kernel::HashJoinBuild;
using cura::kernel::Kernel;
using cura::relational::Rel;
using cura::relational::RelAggregate;
using cura::relational::RelFilter;
using cura::relational::RelHashJoin;
using cura::relational::RelHashJoinBuild;
using cura::relational::RelHashJoinProbe;
using cura::relational::RelInputSource;
using cura::relational::RelLimit;
using cura::relational::RelProject;
using cura::relational::RelSort;
using cura::relational::RelUnion;
using cura::relational::RelUnionAll;
using cura::relational::RelVisitor;

namespace detail {

using cura::kernel::NonStreamKernel;
using cura::kernel::Source;

struct PipelineBuilder {
  std::vector<std::shared_ptr<const Source>> sources;
  std::vector<std::shared_ptr<const NonStreamKernel>> non_streams;

  PipelineBuilder() = default;

  PipelineBuilder(
      std::vector<std::shared_ptr<const Source>> sources_,
      std::vector<std::shared_ptr<const NonStreamKernel>> non_streams_);

  explicit PipelineBuilder(std::vector<PipelineBuilder> &&builders);
};

struct PipelineChain {
  std::list<PipelineBuilder> closed_pipelines;
  std::optional<PipelineBuilder> open_pipeline;
};

PipelineChain mergePipelineChains(std::vector<PipelineChain> &pipeline_chains);

} // namespace detail

/// Generate pipelines for the given Rel.
struct PipelineGenerator
    : RelVisitor<PipelineGenerator, std::shared_ptr<Kernel>> {
  explicit PipelineGenerator(const Option &option_) : option(option_) {}

  std::list<std::unique_ptr<Pipeline>>
  genPipelines(std::shared_ptr<const Rel> rel) &&;

public:
  std::shared_ptr<Kernel>
  visitInputSource(std::shared_ptr<const RelInputSource> input_source,
                   std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitFilter(std::shared_ptr<const RelFilter> filter,
              std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitUnion(std::shared_ptr<const RelUnion> u,
             std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitUnionAll(std::shared_ptr<const RelUnionAll> union_all,
                std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitHashJoin(std::shared_ptr<const RelHashJoin> hash_join,
                std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitHashJoinBuild(std::shared_ptr<const RelHashJoinBuild> hash_join_build,
                     std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitHashJoinProbe(std::shared_ptr<const RelHashJoinProbe> hash_join_probe,
                     std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitProject(std::shared_ptr<const RelProject> project,
               std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitAggregate(std::shared_ptr<const RelAggregate> aggregate,
                 std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitSort(std::shared_ptr<const RelSort> sort,
            std::vector<std::shared_ptr<Kernel>> &children);

  std::shared_ptr<Kernel>
  visitLimit(std::shared_ptr<const RelLimit> sort,
             std::vector<std::shared_ptr<Kernel>> &children);

private:
  template <typename KernelType, typename... Args>
  std::shared_ptr<KernelType> makeKernel(Args &&... args) {
    return std::make_shared<KernelType>(current_kernel_id++,
                                        std::forward<Args>(args)...);
  }

  std::shared_ptr<Kernel>
  combineResult(std::shared_ptr<Kernel> parent,
                std::vector<std::shared_ptr<Kernel>> &children);

private:
  const Option &option;

private:
  KernelId current_kernel_id = 0;
  std::unordered_map<std::shared_ptr<const RelHashJoinBuild>,
                     std::shared_ptr<const HashJoinBuild>>
      hash_join_build_kernels;
  std::unordered_map<std::shared_ptr<Kernel>, detail::PipelineChain>
      pipeline_chains;
};

} // namespace cura::planning
