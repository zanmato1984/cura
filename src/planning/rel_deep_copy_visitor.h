#pragma once

#include "cura/relational/rel_visitor.h"

namespace cura::planning {

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

template <typename Impl>
struct RelDeepCopyVisitor
    : public RelVisitor<RelDeepCopyVisitor<Impl>, std::shared_ptr<const Rel>> {
  std::shared_ptr<const Rel>
  visitInputSource(const std::shared_ptr<const RelInputSource> &input_source,
                   const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyInputSource(input_source, children);
  }

  std::shared_ptr<const Rel>
  visitFilter(const std::shared_ptr<const RelFilter> &filter,
              const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyFilter(filter, children);
  }

  std::shared_ptr<const Rel>
  visitUnion(const std::shared_ptr<const RelUnion> &u,
             const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyUnion(u, children);
  }

  std::shared_ptr<const Rel>
  visitUnionAll(const std::shared_ptr<const RelUnionAll> &union_all,
                const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyUnionAll(union_all, children);
  }

  std::shared_ptr<const Rel>
  visitHashJoin(const std::shared_ptr<const RelHashJoin> &hash_join,
                const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyHashJoin(hash_join, children);
  }

  std::shared_ptr<const Rel> visitHashJoinBuild(
      const std::shared_ptr<const RelHashJoinBuild> &hash_join_build,
      const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyHashJoinBuild(hash_join_build, children);
  }

  std::shared_ptr<const Rel> visitHashJoinProbe(
      const std::shared_ptr<const RelHashJoinProbe> &hash_join_probe,
      const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyHashJoinProbe(hash_join_probe, children);
  }

  std::shared_ptr<const Rel>
  visitProject(const std::shared_ptr<const RelProject> &project,
               const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyProject(project, children);
  }

  std::shared_ptr<const Rel>
  visitAggregate(const std::shared_ptr<const RelAggregate> &aggregate,
                 const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyAggregate(aggregate, children);
  }

  std::shared_ptr<const Rel>
  visitSort(const std::shared_ptr<const RelSort> &sort,
            const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopySort(sort, children);
  }

  std::shared_ptr<const Rel>
  visitLimit(const std::shared_ptr<const RelLimit> &limit,
             const std::vector<std::shared_ptr<const Rel>> &children) {
    return impl().deepCopyLimit(limit, children);
  }

  std::shared_ptr<const Rel>
  defaultVisit(const std::shared_ptr<const Rel> &rel,
               const std::vector<std::shared_ptr<const Rel>> &children) {
    CURA_FAIL("Shouldn't reach here");
  }

  std::shared_ptr<const Rel>
  deepCopyInputSource(const std::shared_ptr<const RelInputSource> &input_source,
                      const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelInputSource>(input_source->sourceId(),
                                            input_source->schema());
  }

  std::shared_ptr<const Rel>
  deepCopyFilter(const std::shared_ptr<const RelFilter> &filter,
                 const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelFilter>(children[0], filter->condition());
  }

  std::shared_ptr<const Rel>
  deepCopyUnion(const std::shared_ptr<const RelUnion> &u,
                const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelUnion>(std::move(children));
  }

  std::shared_ptr<const Rel>
  deepCopyUnionAll(const std::shared_ptr<const RelUnionAll> &union_all,
                   const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelUnionAll>(std::move(children));
  }

  std::shared_ptr<const Rel>
  deepCopyHashJoin(const std::shared_ptr<const RelHashJoin> &hash_join,
                   const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelHashJoin>(hash_join->joinType(), children[0],
                                         children[1], hash_join->condition());
  }

  std::shared_ptr<const Rel> deepCopyHashJoinBuild(
      const std::shared_ptr<const RelHashJoinBuild> &hash_join_build,
      const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelHashJoinBuild>(children[0],
                                              hash_join_build->buildKeys());
  }

  std::shared_ptr<const Rel> deepCopyHashJoinProbe(
      const std::shared_ptr<const RelHashJoinProbe> &hash_join_probe,
      const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelHashJoinProbe>(
        hash_join_probe->joinType(), children[0], children[1],
        hash_join_probe->probeKeys(), hash_join_probe->output(),
        hash_join_probe->buildSide());
  }

  std::shared_ptr<const Rel>
  deepCopyProject(const std::shared_ptr<const RelProject> &project,
                  const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelProject>(children[0], project->expressions());
  }

  std::shared_ptr<const Rel>
  deepCopyAggregate(const std::shared_ptr<const RelAggregate> &aggregate,
                    const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelAggregate>(children[0], aggregate->groups(),
                                          aggregate->aggregations());
  }

  std::shared_ptr<const Rel>
  deepCopySort(const std::shared_ptr<const RelSort> &sort,
               const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelSort>(children[0], sort->sortInfos());
  }

  std::shared_ptr<const Rel>
  deepCopyLimit(const std::shared_ptr<const RelLimit> &limit,
                const std::vector<std::shared_ptr<const Rel>> &children) {
    return std::make_shared<RelLimit>(children[0], limit->offset(), limit->n());
  }

private:
  RelDeepCopyVisitor() = default;
  friend Impl;

  Impl &impl() { return *static_cast<Impl *>(this); }

  const Impl &impl() const { return *static_cast<const Impl *>(this); }
};

} // namespace cura::planning