#pragma once

#include "cura/common/errors.h"
#include "cura/common/types.h"
#include "cura/execution/context.h"

#include <memory>
#include <string>
#include <vector>

namespace cura::data {
struct Fragment;
} // namespace cura::data

namespace cura::kernel {

using cura::data::Fragment;
using cura::execution::Context;

struct Kernel {
  explicit Kernel(KernelId id_) : id(id_) {}
  virtual ~Kernel() = default;

  virtual std::string name() const = 0;

  virtual std::string toString() const {
    return name() + "#" + std::to_string(id);
  }

  const KernelId id;
};

struct NonTerminalKernel : public Kernel {
  using Kernel::Kernel;

  /// For a heap source, fragment must be null.
  virtual void push(const Context &ctx, ThreadId thread_id, KernelId upstream,
                    std::shared_ptr<const Fragment> fragment) const = 0;

  /// For a heap source, fragment must be null.
  /// And `rows` is used to specify how much rows to stream out so that some
  /// kind of parallel fetching scheme can be achieved. Otherwise `rows` is
  /// ignored. Parameter `upstream` is used for cases like sort merge join.
  virtual std::shared_ptr<const Fragment>
  stream(const Context &ctx, ThreadId thread_id, KernelId upstream,
         std::shared_ptr<const Fragment> fragment, size_t rows) const = 0;
};

struct NonStreamKernel : public NonTerminalKernel {
  using NonTerminalKernel::NonTerminalKernel;

  std::shared_ptr<const Fragment>
  stream(const Context &ctx, ThreadId thread_id, KernelId upstream,
         std::shared_ptr<const Fragment> fragment,
         size_t rows) const override final {
    CURA_FAIL("stream is not allowed for a " + name() + " kernel");
  }

  virtual void concatenate(const Context &ctx) const = 0;

  virtual void converge(const Context &ctx) const = 0;
};

struct HeapNonStreamKernel : public NonStreamKernel {
  using NonStreamKernel::NonStreamKernel;

  virtual std::shared_ptr<const Fragment> heapFragment() const = 0;
};

struct StreamKernel : public NonTerminalKernel {
  using NonTerminalKernel::NonTerminalKernel;

  void push(const Context &ctx, ThreadId thread_id, KernelId upstream,
            std::shared_ptr<const Fragment> fragment) const override final;

  std::shared_ptr<const Fragment>
  stream(const Context &ctx, ThreadId thread_id, KernelId upstream,
         std::shared_ptr<const Fragment> fragment,
         size_t rows) const override final;

protected:
  virtual std::shared_ptr<const Fragment>
  pushImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
           std::shared_ptr<const Fragment> fragment) const = 0;

  virtual std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment, size_t rows) const = 0;

public:
  std::shared_ptr<const NonTerminalKernel> downstream = nullptr;
};

struct NonSourceStreamKernel : public StreamKernel {
  using StreamKernel::StreamKernel;

protected:
  std::shared_ptr<const Fragment>
  pushImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
           std::shared_ptr<const Fragment> fragment) const override final;

  std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment,
             size_t rows) const override final;

  virtual std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment) const = 0;
};

struct Terminal : public Kernel {
  explicit Terminal(KernelId id,
                    std::vector<std::shared_ptr<const NonStreamKernel>> kernels)
      : Kernel(id), non_stream_kernels(std::move(kernels)) {
    CURA_ASSERT(!non_stream_kernels.empty(),
                "No kernels in " + name() + " kernel");
  }

  std::string name() const override { return "Terminal"; }

  std::vector<std::shared_ptr<const NonStreamKernel>> non_stream_kernels;
};

} // namespace cura::kernel
