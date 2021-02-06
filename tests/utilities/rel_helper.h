#pragma once

#include "cura/relational/rels.h"

namespace cura::test::relational {

using cura::relational::Rel;

template <typename RelType, typename... Args>
inline std::shared_ptr<const Rel> makeRel(Args &&... args) {
  return std::make_shared<RelType>(std::forward<Args>(args)...);
}

std::string toJson(std::shared_ptr<const Rel> rel);

} // namespace cura::test::relational
