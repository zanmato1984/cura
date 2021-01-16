#include "cura/kernel/filter.h"
#include "cura/data/column_scalar.h"
#include "cura/data/column_vector.h"
#include "cura/data/fragment.h"

#include <arrow/compute/api.h>
#include <arrow/visitor_inline.h>

#ifdef USE_CUDF
#include <cudf/stream_compaction.hpp>
#endif

namespace cura::kernel {

using cura::data::ColumnScalar;
using cura::data::ColumnVector;
using cura::type::TypeId;

namespace detail {

struct ScalarVisitor {
  arrow::Status Visit(const arrow::BooleanScalar &scalar) {
    value = scalar.value;
    return arrow::Status::OK();
  }

  template <typename T>
  typename std::enable_if_t<!std::is_same_v<arrow::BooleanScalar, T>,
                            arrow::Status>
  Visit(const T &scalar) {
    return arrow::Status::NotImplemented("ScalarVisitor not implemented for " +
                                         scalar.ToString());
  }

  bool value = false;
};

} // namespace detail

std::shared_ptr<const Fragment>
Filter::streamImpl(const Context &ctx, ThreadId thread_id, KernelId upstream,
                   std::shared_ptr<const Fragment> fragment) const {
  auto mask = condition->evaluate(ctx, thread_id, *fragment);
  CURA_ASSERT(mask->dataType().type_id == TypeId::BOOL8,
              "Mask must be bool type");

  if (auto mask_cs = std::dynamic_pointer_cast<const ColumnScalar>(mask);
      mask_cs) {
    detail::ScalarVisitor visitor;
    CURA_ASSERT_ARROW_OK(arrow::VisitScalarInline(*mask_cs->arrow(), &visitor),
                         "Get arrow scalar value failed");
    return visitor.value ? fragment : nullptr;
  }

  auto mask_cv = std::dynamic_pointer_cast<const ColumnVector>(mask);
#ifdef USE_CUDF
  auto filtered =
      cudf::apply_boolean_mask(fragment->cudf(), mask_cv->cudf(),
                               ctx.memory_resource->preConcatenate(thread_id));
  return std::make_shared<Fragment>(schema, std::move(filtered));
#else
  arrow::compute::ExecContext context(
      ctx.memory_resource->preConcatenate(thread_id));
  const auto &filtered = CURA_GET_ARROW_RESULT(arrow::compute::Filter(
      fragment->arrow(), mask_cv->arrow(),
      arrow::compute::FilterOptions::Defaults(), &context));
  CURA_ASSERT(filtered.kind() == arrow::Datum::RECORD_BATCH,
              "Filter result must be an arrow record batch");
  if (filtered.record_batch()->num_rows() == 0) {
    return nullptr;
  }
  return std::make_shared<Fragment>(filtered.record_batch());
#endif
}

} // namespace cura::kernel
