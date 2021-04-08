#pragma once

#include "cura/common/errors.h"
#include "cura/relational/rels.h"

#include <memory>

namespace cura::relational {

template <typename Impl, typename Result> struct RelVisitor {
  Result visit(const std::shared_ptr<const Rel> &rel) {
    if constexpr (std::is_same_v<Result, void>) {
      for (const auto &input : rel->inputs) {
        visit(input);
      }
      dispatch(rel, nullptr);
    } else {
      std::vector<Result> children;
      for (const auto &input : rel->inputs) {
        children.emplace_back(visit(input));
      }
      return dispatch(rel, children);
    }
  }

#define APPLY_FOR_RELS(ACTION)                                                 \
  ACTION(InputSource)                                                          \
  ACTION(Filter)                                                               \
  ACTION(Union)                                                                \
  ACTION(UnionAll)                                                             \
  ACTION(HashJoin)                                                             \
  ACTION(HashJoinBuild)                                                        \
  ACTION(HashJoinProbe)                                                        \
  ACTION(Project)                                                              \
  ACTION(Aggregate)                                                            \
  ACTION(Sort)                                                                 \
  ACTION(Limit)

#define DEF_VISIT_REL(REL)                                                     \
  Result visit##REL(const std::shared_ptr<const Rel##REL> &rel,                \
                    const std::vector<Result> &children) {                     \
    return impl().defaultVisit(rel, children);                                 \
  }                                                                            \
  void visit##REL(const std::shared_ptr<const Rel##REL> &rel) {                \
    impl().defaultVisit(rel);                                                  \
  }

  APPLY_FOR_RELS(DEF_VISIT_REL)

#undef DEF_VISIT_REL

  Result defaultVisit(const std::shared_ptr<const Rel> &,
                      const std::vector<Result> &children) {
    return {};
  }

  void defaultVisit(const std::shared_ptr<const Rel> &) {}

private:
  RelVisitor() = default;
  friend Impl;

  Impl &impl() { return *static_cast<Impl *>(this); }

  const Impl &impl() const { return *static_cast<const Impl *>(this); }

  template <typename ChildrenResult =
                std::conditional<std::is_same_v<Result, void>, void *,
                                 const std::vector<Result> &>>
  Result dispatch(const std::shared_ptr<const Rel> &rel,
                  ChildrenResult children) {
#define REL_CASE(REL)                                                          \
  if (auto rel_##REL = std::dynamic_pointer_cast<const Rel##REL>(rel);         \
      rel_##REL) {                                                             \
    if constexpr (std::is_same_v<Result, void>) {                              \
      impl().visit##REL(rel_##REL);                                            \
      return;                                                                  \
    } else {                                                                   \
      return impl().visit##REL(rel_##REL, children);                           \
    }                                                                          \
  } else

    APPLY_FOR_RELS(REL_CASE)

#undef REL_CASE

    CURA_FAIL("Unknown Rel type.");
  }
};

} // namespace cura::relational