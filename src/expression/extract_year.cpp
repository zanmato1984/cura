#include "extract_year.h"
#include "cura/data/column_factories.h"

#include <arrow/api.h>

namespace cura::expression::detail {

using cura::data::createArrowColumnVector;
using cura::type::TypeId;

std::shared_ptr<const Column>
extractYear(const Context &ctx, ThreadId thread_id,
            std::shared_ptr<const ColumnVector> cv,
            const DataType &result_type) {
  CURA_ASSERT(cv->dataType().type_id == TypeId::INT64,
              "Extract year requires int64 operand");
  CURA_ASSERT(result_type.type_id == TypeId::INT64,
              "Extract year's result should be int64");

  const auto &input_array = cv->arrow();
  auto output_array = [&]() {
    std::vector<std::shared_ptr<arrow::Buffer>> buffers;
    buffers.emplace_back(input_array->null_bitmap());
    auto buf = CURA_GET_ARROW_RESULT(
        arrow::AllocateBuffer(cv->size() * sizeof(int64_t),
                              ctx.memory_resource->preConcatenate(thread_id)));
    buffers.emplace_back(std::move(buf));
    auto data = arrow::ArrayData::Make(result_type.arrow(), cv->size(),
                                       std::move(buffers));
    return arrow::MakeArray(data);
  }();

  auto in_data = input_array->data()->GetValues<int64_t>(1);
  auto out_data = output_array->data()->GetMutableValues<int64_t>(1);
  for (size_t i = 0; i < cv->size(); i++) {
    out_data[i] = (static_cast<uint64_t>(in_data[i]) & YEAR_BIT_FIELD_MASK) >>
                  YEAR_BIT_FIELD_OFFSET;
  }

  return createArrowColumnVector(result_type, output_array);
}

} // namespace cura::expression::detail
