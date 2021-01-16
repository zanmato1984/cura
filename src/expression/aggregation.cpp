#include "cura/expression/aggregation.h"

namespace cura::expression {

std::shared_ptr<const Column>
Aggregation::evaluate(const Context &ctx, ThreadId thread_id,
                      const Fragment &fragment) const {
  CURA_FAIL("Shouldn't call Aggregation evaluate directly");
}

} // namespace cura::expression
