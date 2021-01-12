#pragma once

#include "cura/common/utilities.h"
#include "cura/kernel/kernel.h"

#include <mutex>

namespace cura::kernel {

using cura::data::Fragment;

struct Source : public StreamKernel {
  explicit Source(KernelId id) : StreamKernel(id) {}

  virtual SourceId sourceId() const = 0;
};

struct InputSource : public Source {
  explicit InputSource(KernelId id, SourceId source_id_)
      : Source(id), source_id(source_id_) {}

  std::string name() const override { return "InputSource"; }

  std::string toString() const override {
    return Kernel::toString() + "(" + std::to_string(source_id) + ")";
  }

  SourceId sourceId() const override { return source_id; }

protected:
  std::shared_ptr<const Fragment>
  pushImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
           std::shared_ptr<const Fragment> fragment) const override;

  std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment,
             size_t rows) const override;

private:
  const SourceId source_id;
};

/// Data of a heap source is on heap already, it has significant different
/// schemes between of push and of stream. For push, we want to keep the
/// contiguous data as is since scattering and re-gathering it will be totally
/// wasteful in terms of both performance and memory footprint. For stream, we
/// want to enable the scheme that parallel fetches different portion of the
/// data on heap. Therefore this class has significant different `push`/`stream`
/// methods than both `InputSource` and `NonSourceStreamKernel`.
struct HeapSource : public Source {
  explicit HeapSource(KernelId id,
                      std::shared_ptr<const HeapNonStreamKernel> kernel_)
      : Source(id), kernel(kernel_) {}

  std::string name() const override { return "HeapSource"; }

  std::string toString() const override {
    return Kernel::toString() + "(" + std::to_string(kernel->id) + ")";
  }

  SourceId sourceId() const override {
    return makeHeapSourceId(static_cast<SourceId>(id));
  }

protected:
  std::shared_ptr<const Fragment>
  pushImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
           std::shared_ptr<const Fragment> fragment) const override;

  std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment,
             size_t rows) const override;

private:
  std::shared_ptr<const HeapNonStreamKernel> kernel;

  /// For push usage.
  mutable std::mutex push_mutex;
  mutable bool pushed = false;

  /// For stream usage.
  mutable std::mutex stream_mutex;
  mutable size_t current_row = 0;
};

} // namespace cura::kernel
