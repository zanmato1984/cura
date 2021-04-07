#include "explainer.h"

namespace cura::planning {

std::vector<std::string>
Explainer::defaultVisit(const std::shared_ptr<const Rel> &rel,
                        const std::vector<std::vector<std::string>> &children) {
  std::vector<std::string> combined;
  combined.emplace_back(std::move(rel->toString()));
  for (size_t i = 0; i < children.size(); i++) {
    for (size_t j = 0; j < children[i].size(); j++) {
      if (j == 0) {
        combined.emplace_back("|- " + children[i][j]);
      } else if (i != children.size() - 1) {
        combined.emplace_back("|  " + children[i][j]);
      } else {
        combined.emplace_back("   " + children[i][j]);
      }
    }
  }
  return combined;
}

} // namespace cura::planning
