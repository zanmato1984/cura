#pragma once

#include "rel_deep_copy_visitor.h"

namespace cura::planning {

/// Break HashJoin within the given Rel.
struct HashJoinBreaker : public RelDeepCopyVisitor<HashJoinBreaker> {
  using Base = RelDeepCopyVisitor<HashJoinBreaker>;
  std::shared_ptr<const Rel>
  deepCopyHashJoin(std::shared_ptr<const RelHashJoin> hash_join,
                   std::vector<std::shared_ptr<const Rel>> &children);
};

} // namespace cura::planning