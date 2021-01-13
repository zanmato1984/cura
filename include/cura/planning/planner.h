#pragma once

#include <list>
#include <memory>
#include <string>
#include <vector>

namespace cura::driver {
struct Option;
} // namespace cura::driver

namespace cura::execution {
struct Pipeline;
} // namespace cura::execution

namespace cura::relational {
struct Rel;
} // namespace cura::relational

namespace cura::planning {

using cura::driver::Option;
using cura::execution::Pipeline;
using cura::relational::Rel;

struct Planner {
  explicit Planner(const Option &option_) : option(option_) {}

  std::list<std::unique_ptr<Pipeline>>
  plan(std::shared_ptr<const Rel> rel) const;

  std::vector<std::string> explain(std::shared_ptr<const Rel> rel,
                                   bool extended = false) const;

private:
  const Option &option;
};

} // namespace cura::planning