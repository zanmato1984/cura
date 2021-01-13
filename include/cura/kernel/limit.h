#pragma once

#include "cura/expression/expressions.h"
#include "cura/kernel/kernel.h"
#include "cura/relational/rels.h"
#include "cura/type/data_type.h"

#include <mutex>

namespace cura::kernel {

struct Limit : public NonSourceStreamKernel {
  explicit Limit(KernelId id, size_t offset_, size_t n_)
      : NonSourceStreamKernel(id), offset(offset_), n(n_), begin(offset_),
        remaining(n_) {}

  std::string name() const override { return "Limit"; }

  virtual std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment) const override;

  std::string toString() const override {
    return Kernel::toString() + "(" +
           (offset == 0 ? "" : (std::to_string(offset) + ", ")) +
           std::to_string(n) + ")";
  }

private:
  size_t offset;
  size_t n;

  mutable std::mutex mutex;
  mutable size_t begin;
  mutable size_t remaining;
};

} // namespace cura::kernel
