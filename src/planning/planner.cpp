#include "cura/planning/planner.h"
#include "cura/execution/pipeline.h"
#include "explainer.h"
#include "hash_join_breaker.h"
#include "pipeline_generator.h"
#include "validators.h"

namespace cura::planning {

std::list<std::unique_ptr<Pipeline>>
Planner::plan(const std::shared_ptr<const Rel> &rel) const {
  /// Basic validations on the given Rel.
  ColumnRefValidator().visit(rel);

  /// Break HashJoin within the given Rel.
  auto new_rel = HashJoinBreaker().visit(rel);

  // TODO: Should transform Sort+Limit to TopN when we have a decent TopN
  // kernel.

  /// Generate pipelines for the given Rel.
  return PipelineGenerator(option).genPipelines(new_rel);
}

std::vector<std::string> Planner::explain(const std::shared_ptr<const Rel> &rel,
                                          bool extended) const {
  /// Basic validations on the given Rel.
  ColumnRefValidator().visit(rel);

  /// Header.
#ifdef USE_CUDF
  auto result = std::vector<std::string>{"CURA GPU Plan"};
#else
  auto result = std::vector<std::string>{"CURA CPU Plan"};
#endif

  /// Logical explain of the given Rel.
  {
    auto logical = Explainer().visit(rel);
    result.insert(result.end(), std::make_move_iterator(logical.begin()),
                  std::make_move_iterator(logical.end()));
  }

  /// Physical explain of the generated pipelines.
  if (extended) {
    /// Break HashJoin within the given Rel.
    auto new_rel = HashJoinBreaker().visit(rel);

    // TODO: Should transform Sort+Limit to TopN when we have a decent TopN
    // kernel.

    /// Generate pipelines for the given Rel.
    auto pipelines = PipelineGenerator(option).genPipelines(new_rel);

    for (const auto &pipeline : pipelines) {
      result.emplace_back(pipeline->toString());
    }
  }

  return result;
}

} // namespace cura::planning