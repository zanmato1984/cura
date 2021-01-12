#pragma once

#include "cura/common/errors.h"
#include "cura/relational/rels.h"

#include <memory>

namespace cura::relational {

template <typename Impl, typename Result> struct RelVisitor {
  Result visit(std::shared_ptr<const Rel> rel) {
    std::vector<Result> children;
    for (const auto &input : rel->inputs) {
      children.emplace_back(visit(input));
    }
    return dispatch(rel, children);
  }

  Result visitInputSource(std::shared_ptr<const RelInputSource> input_source,
                          std::vector<Result> &children) {
    return impl().defaultVisit(input_source, children);
  }

  Result visitFilter(std::shared_ptr<const RelFilter> filter,
                     std::vector<Result> &children) {
    return impl().defaultVisit(filter, children);
  }

  Result visitUnion(std::shared_ptr<const RelUnion> u,
                    std::vector<Result> &children) {
    return impl().defaultVisit(u, children);
  }

  Result visitUnionAll(std::shared_ptr<const RelUnionAll> union_all,
                       std::vector<Result> &children) {
    return impl().defaultVisit(union_all, children);
  }

  Result visitHashJoin(std::shared_ptr<const RelHashJoin> hash_join,
                       std::vector<Result> &children) {
    return impl().defaultVisit(hash_join, children);
  }

  Result
  visitHashJoinBuild(std::shared_ptr<const RelHashJoinBuild> hash_join_build,
                     std::vector<Result> &children) {
    return impl().defaultVisit(hash_join_build, children);
  }

  Result
  visitHashJoinProbe(std::shared_ptr<const RelHashJoinProbe> hash_join_probe,
                     std::vector<Result> &children) {
    return impl().defaultVisit(hash_join_probe, children);
  }

  Result visitProject(std::shared_ptr<const RelProject> project,
                      std::vector<Result> &children) {
    return impl().defaultVisit(project, children);
  }

  Result visitAggregate(std::shared_ptr<const RelAggregate> aggregate,
                        std::vector<Result> &children) {
    return impl().defaultVisit(aggregate, children);
  }

  Result visitSort(std::shared_ptr<const RelSort> sort,
                   std::vector<Result> &children) {
    return impl().defaultVisit(sort, children);
  }

  Result visitLimit(std::shared_ptr<const RelLimit> limit,
                    std::vector<Result> &children) {
    return impl().defaultVisit(limit, children);
  }

  Result defaultVisit(std::shared_ptr<const Rel>,
                      std::vector<Result> &children) {
    return {};
  }

private:
  RelVisitor() = default;
  friend Impl;

  Impl &impl() { return *static_cast<Impl *>(this); }

  const Impl &impl() const { return *static_cast<const Impl *>(this); }

  Result dispatch(std::shared_ptr<const Rel> rel,
                  std::vector<Result> &children) {
    if (auto input_source =
            std::dynamic_pointer_cast<const RelInputSource>(rel);
        input_source) {
      return impl().visitInputSource(input_source, children);
    }

    if (auto filter = std::dynamic_pointer_cast<const RelFilter>(rel); filter) {
      return impl().visitFilter(filter, children);
    }

    if (auto u = std::dynamic_pointer_cast<const RelUnion>(rel); u) {
      return impl().visitUnion(u, children);
    }

    if (auto union_all = std::dynamic_pointer_cast<const RelUnionAll>(rel);
        union_all) {
      return impl().visitUnionAll(union_all, children);
    }

    if (auto hash_join = std::dynamic_pointer_cast<const RelHashJoin>(rel);
        hash_join) {
      return impl().visitHashJoin(hash_join, children);
    }

    if (auto hash_join_build =
            std::dynamic_pointer_cast<const RelHashJoinBuild>(rel);
        hash_join_build) {
      return impl().visitHashJoinBuild(hash_join_build, children);
    }

    if (auto hash_join_probe =
            std::dynamic_pointer_cast<const RelHashJoinProbe>(rel);
        hash_join_probe) {
      return impl().visitHashJoinProbe(hash_join_probe, children);
    }

    if (auto project = std::dynamic_pointer_cast<const RelProject>(rel);
        project) {
      return impl().visitProject(project, children);
    }

    if (auto aggregate = std::dynamic_pointer_cast<const RelAggregate>(rel);
        aggregate) {
      return impl().visitAggregate(aggregate, children);
    }

    if (auto sort = std::dynamic_pointer_cast<const RelSort>(rel); sort) {
      return impl().visitSort(sort, children);
    }

    if (auto limit = std::dynamic_pointer_cast<const RelLimit>(rel); limit) {
      return impl().visitLimit(limit, children);
    }

    CURA_FAIL("Unknown Rel type.");
  }
};

template <typename Impl> struct RelVisitor<Impl, void> {
  void visit(std::shared_ptr<const Rel> rel) {
    for (const auto &input : rel->inputs) {
      visit(input);
    }
    dispatch(rel);
  }

  void visitInputSource(std::shared_ptr<const RelInputSource> input_source) {
    impl().defaultVisit(input_source);
  }

  void visitFilter(std::shared_ptr<const RelFilter> filter) {
    impl().defaultVisit(filter);
  }

  void visitUnion(std::shared_ptr<const RelUnion> u) { impl().defaultVisit(u); }

  void visitUnionAll(std::shared_ptr<const RelUnionAll> union_all) {
    impl().defaultVisit(union_all);
  }

  void visitHashJoin(std::shared_ptr<const RelHashJoin> hash_join) {
    impl().defaultVisit(hash_join);
  }

  void
  visitHashJoinBuild(std::shared_ptr<const RelHashJoinBuild> hash_join_build) {
    impl().defaultVisit(hash_join_build);
  }

  void
  visitHashJoinProbe(std::shared_ptr<const RelHashJoinProbe> hash_join_probe) {
    impl().defaultVisit(hash_join_probe);
  }

  void visitProject(std::shared_ptr<const RelProject> project) {
    return impl().defaultVisit(project);
  }

  void visitAggregate(std::shared_ptr<const RelAggregate> aggregate) {
    return impl().defaultVisit(aggregate);
  }

  void visitSort(std::shared_ptr<const RelSort> sort) {
    return impl().defaultVisit(sort);
  }

  void visitLimit(std::shared_ptr<const RelLimit> limit) {
    return impl().defaultVisit(limit);
  }

  void defaultVisit(std::shared_ptr<const Rel> rel) {}

private:
  RelVisitor() = default;
  friend Impl;

  Impl &impl() { return *static_cast<Impl *>(this); }

  const Impl &impl() const { return *static_cast<const Impl *>(this); }

  void dispatch(std::shared_ptr<const Rel> rel) {
    if (auto input_source =
            std::dynamic_pointer_cast<const RelInputSource>(rel);
        input_source) {
      impl().visitInputSource(input_source);
      return;
    }

    if (auto filter = std::dynamic_pointer_cast<const RelFilter>(rel); filter) {
      impl().visitFilter(filter);
      return;
    }

    if (auto u = std::dynamic_pointer_cast<const RelUnion>(rel); u) {
      impl().visitUnion(u);
      return;
    }

    if (auto union_all = std::dynamic_pointer_cast<const RelUnionAll>(rel);
        union_all) {
      impl().visitUnionAll(union_all);
      return;
    }

    if (auto hash_join = std::dynamic_pointer_cast<const RelHashJoin>(rel);
        hash_join) {
      impl().visitHashJoin(hash_join);
      return;
    }

    if (auto hash_join_build =
            std::dynamic_pointer_cast<const RelHashJoinBuild>(rel);
        hash_join_build) {
      impl().visitHashJoinBuild(hash_join_build);
      return;
    }

    if (auto hash_join_probe =
            std::dynamic_pointer_cast<const RelHashJoinProbe>(rel);
        hash_join_probe) {
      impl().visitHashJoinProbe(hash_join_probe);
      return;
    }

    if (auto project = std::dynamic_pointer_cast<const RelProject>(rel);
        project) {
      impl().visitProject(project);
      return;
    }

    if (auto aggregate = std::dynamic_pointer_cast<const RelAggregate>(rel);
        aggregate) {
      impl().visitAggregate(aggregate);
      return;
    }

    if (auto sort = std::dynamic_pointer_cast<const RelSort>(rel); sort) {
      impl().visitSort(sort);
      return;
    }

    if (auto limit = std::dynamic_pointer_cast<const RelLimit>(rel); limit) {
      impl().visitLimit(limit);
      return;
    }

    CURA_FAIL("Unknown Rel type.");
  }
};

} // namespace cura::relational