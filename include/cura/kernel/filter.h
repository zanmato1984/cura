#pragma once

#include "cura/expression/expressions.h"
#include "cura/kernel/kernel.h"

namespace cura::kernel {

using cura::type::Schema;

using cura::expression::Expression;

struct Filter : NonSourceStreamKernel {
  Filter(KernelId id, Schema schema_,
         std::shared_ptr<const Expression> condition_)
      : NonSourceStreamKernel(id), schema(std::move(schema_)),
        condition(condition_) {}

  std::string name() const override { return "Filter"; }

  std::string toString() const override {
    return Kernel::toString() + "(" + condition->toString() + ")";
  }

protected:
  std::shared_ptr<const Fragment>
  streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
             std::shared_ptr<const Fragment> fragment) const override;

private:
  Schema schema;
  std::shared_ptr<const Expression> condition;
};

} // namespace cura::kernel
