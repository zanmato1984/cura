#pragma once

#include "cura/relational/rel_visitor.h"

namespace cura::planning {

using cura::relational::RelAggregate;
using cura::relational::RelFilter;
using cura::relational::RelHashJoin;
using cura::relational::RelProject;
using cura::relational::RelSort;
using cura::relational::RelVisitor;

/// Validate that ColumnRef is within the bound of the children's output and of
/// the same data type as the referred column.
struct ColumnRefValidator : public RelVisitor<ColumnRefValidator, void> {
  void visitFilter(const std::shared_ptr<const RelFilter> &filter);

  void visitHashJoin(const std::shared_ptr<const RelHashJoin> &hash_join);

  void visitProject(const std::shared_ptr<const RelProject> &project);

  void visitAggregate(const std::shared_ptr<const RelAggregate> &aggregate);

  void visitSort(const std::shared_ptr<const RelSort> &sort);
};

// TODO: Type check and inference.

} // namespace cura::planning