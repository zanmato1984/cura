#include "cura/kernel/sources.h"
#include "cura/data/column_vector.h"
#include "cura/data/fragment.h"

namespace cura::kernel {

using cura::data::Column;

std::shared_ptr<const Fragment>
InputSource::pushImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
                      std::shared_ptr<const Fragment> fragment) const {
  return fragment;
}

std::shared_ptr<const Fragment> InputSource::streamImpl(
    const Context &ctx, ThreadId thread_id, KernelId upstream,
    std::shared_ptr<const Fragment> fragment, size_t rows) const {
  return fragment;
}

std::shared_ptr<const Fragment>
HeapSource::pushImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
                     std::shared_ptr<const Fragment> fragment) const {
  CURA_ASSERT(!fragment, "fragment for " + name() + "'s pushImpl must be null");

  auto heap_fragment = kernel->heapFragment();
  if (pushed) {
    return nullptr;
  }

  {
    std::lock_guard lock(push_mutex);
    if (pushed) {
      return nullptr;
    }
    pushed = true;
  }

  return heap_fragment;
}

std::shared_ptr<const Fragment> HeapSource::streamImpl(
    const Context &ctx, ThreadId thread_id, KernelId upstream,
    std::shared_ptr<const Fragment> fragment, size_t rows) const {
  CURA_ASSERT(!fragment,
              "fragment for " + name() + "'s streamImpl must be null");

  auto heap_fragment = kernel->heapFragment();
  if (current_row >= heap_fragment->size()) {
    return nullptr;
  }

  size_t last_row;
  {
    std::lock_guard lock(stream_mutex);
    if (current_row >= heap_fragment->size()) {
      return nullptr;
    }
    last_row = current_row;
    current_row += rows;
  }

  std::vector<std::shared_ptr<const Column>> columns;
  for (size_t i = 0; i < heap_fragment->numColumns(); i++) {
    columns.emplace_back(heap_fragment->column(i)->view(last_row, rows));
  }

  return std::make_shared<Fragment>(std::move(columns));
}

} // namespace cura::kernel