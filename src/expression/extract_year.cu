#include "cura/data/column_factories.h"
#include "extract_year.h"

#include <cudf/copying.hpp>
#include <cudf/utilities/error.hpp>

namespace cura::expression::detail {

using cura::data::ColumnVectorCudfColumn;
using cura::data::createCudfColumnVector;
using cura::type::TypeId;

struct DeviceExtractYear {
  __device__ int64_t operator()(int64_t data) {
    return (static_cast<uint64_t>(data) & YEAR_BIT_FIELD_MASK) >>
           YEAR_BIT_FIELD_OFFSET;
  }
};

std::shared_ptr<const Column>
extractYear(const Context &ctx, ThreadId thread_id,
            std::shared_ptr<const ColumnVector> cv,
            const DataType &result_type) {
  CURA_ASSERT(cv->dataType().type_id == TypeId::INT64,
              "Extract year requires int64 operand");
  CURA_ASSERT(result_type.type_id == TypeId::INT64,
              "Extract year's result should be int64");

  auto input = cv->cudf();
  auto output = cudf::allocate_like(
      input, input.size(), cudf::mask_allocation_policy::NEVER,
      ctx.memory_resource->preConcatenate(thread_id));

  if (input.size() == 0) {
    return createCudfColumnVector<ColumnVectorCudfColumn>(result_type,
                                                          std::move(output));
  }

  auto output_view = output->mutable_view();
  if (cv->dataType().nullable) {
    output->set_null_mask(
        rmm::device_buffer{input.null_mask(),
                           cudf::bitmask_allocation_size_bytes(input.size())},
        input.null_count());
  }

  thrust::transform(rmm::exec_policy(0)->on(0), input.begin<int64_t>(),
                    input.end<int64_t>(), output_view.begin<int64_t>(),
                    DeviceExtractYear{});

  CHECK_CUDA(0);

  return createCudfColumnVector<ColumnVectorCudfColumn>(result_type,
                                                        std::move(output));
}

} // namespace cura::expression::detail
