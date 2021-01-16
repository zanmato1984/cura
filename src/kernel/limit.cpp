#include "cura/kernel/limit.h"
#include "cura/data/column_vector.h"
#include "cura/data/fragment.h"

namespace cura::kernel {

using cura::data::Column;

std::shared_ptr<const Fragment>
Limit::streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
                  std::shared_ptr<const Fragment> fragment) const {
  if (remaining == 0) {
    return nullptr;
  }

  size_t to_begin;
  size_t to_emit;
  {
    std::lock_guard lock(mutex);
    if (remaining == 0) {
      return nullptr;
    }

    if (fragment->size() <= begin) {
      begin -= fragment->size();
      return nullptr;
    }

    to_begin = begin;
    if (begin + remaining <= fragment->size()) {
      to_emit = remaining;
    } else {
      to_emit = fragment->size() - begin;
    }

    begin = 0;
    remaining -= to_emit;
  }

  std::vector<std::shared_ptr<const Column>> columns;
  for (size_t i = 0; i < fragment->numColumns(); i++) {
    columns.emplace_back(fragment->column(i)->view(to_begin, to_emit));
  }
  return std::make_shared<Fragment>(std::move(columns));
}

} // namespace cura::kernel