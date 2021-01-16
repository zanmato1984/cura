#include "cura/kernel/project.h"
#include "cura/data/fragment.h"

namespace cura::kernel {

using cura::data::Column;

std::shared_ptr<const Fragment>
Project::streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
                    std::shared_ptr<const Fragment> fragment) const {
  std::vector<std::shared_ptr<const Column>> columns(expressions.size());
  std::transform(
      expressions.begin(), expressions.end(), columns.begin(),
      [&](const auto &e) { return e->evaluate(ctx, thread_id, *fragment); });
  return std::make_shared<Fragment>(std::move(columns));
}

} // namespace cura::kernel