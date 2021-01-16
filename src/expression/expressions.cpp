#include "cura/expression/expressions.h"
#include "cura/data/column_vector.h"
#include "cura/data/fragment.h"

namespace cura::expression {

std::shared_ptr<const Column>
ColumnRef::evaluate(const Context &ctx, ThreadId thread_id,
                    const Fragment &fragment) const {
  CURA_ASSERT(idx < fragment.numColumns(), "Column idx overflow");
  CURA_ASSERT(fragment.column(idx)->dataType() == data_type,
              "Mismatched data type " +
                  fragment.column(idx)->dataType().toString() + " vs. " +
                  data_type.toString());
  return fragment.column(idx);
}

} // namespace cura::expression