#pragma once

#include "cura/relational/rel_visitor.h"

namespace cura::planning {

using cura::relational::Rel;
using cura::relational::RelVisitor;

struct Explainer : public RelVisitor<Explainer, std::vector<std::string>> {
  std::vector<std::string>
  defaultVisit(const std::shared_ptr<const Rel> &rel,
               const std::vector<std::vector<std::string>> &children);
};

} // namespace cura::planning
