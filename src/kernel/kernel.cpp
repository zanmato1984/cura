#include "cura/kernel/kernel.h"
#include "cura/data/fragment.h"

namespace cura::kernel {

void StreamKernel::push(const Context &ctx, ThreadId thread_id,
                        KernelId upstream,
                        std::shared_ptr<const Fragment> fragment) const {
  CURA_ASSERT(downstream, "push is not allowed for " + name() +
                              " kernel without a downstream");
  auto result = pushImpl(ctx, thread_id, upstream, fragment);
  if (result) {
    downstream->push(ctx, thread_id, id, result);
  }
}

std::shared_ptr<const Fragment>
StreamKernel::stream(const Context &ctx, ThreadId thread_id, KernelId upstream,
                     std::shared_ptr<const Fragment> fragment,
                     size_t rows) const {
  auto result = streamImpl(ctx, thread_id, upstream, fragment, rows);
  if (result && downstream) {
    return downstream->stream(ctx, thread_id, id, result, rows);
  }
  return result;
}

std::shared_ptr<const Fragment> NonSourceStreamKernel::pushImpl(
    const Context &ctx, ThreadId thread_id, KernelId upstream,
    std::shared_ptr<const Fragment> fragment) const {
  return streamImpl(ctx, thread_id, upstream, fragment);
}

std::shared_ptr<const Fragment> NonSourceStreamKernel::streamImpl(
    const Context &ctx, ThreadId thread_id, KernelId upstream,
    std::shared_ptr<const Fragment> fragment, size_t rows) const {
  return streamImpl(ctx, thread_id, upstream, fragment);
}

} // namespace cura::kernel