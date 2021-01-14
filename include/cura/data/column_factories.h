#pragma once

#include "cura/type/data_type.h"

#include <arrow/api.h>

#ifdef USE_CUDF
#include <cudf/column/column.hpp>
#endif

namespace cura::data {

using cura::type::DataType;

class ColumnScalar;
class ColumnVector;

#ifdef USE_CUDF
std::unique_ptr<ColumnScalar>
createCudfColumnScalar(const DataType &data_type, size_t size,
                       std::shared_ptr<const cudf::scalar> scalar);

template <typename T, typename C>
std::unique_ptr<ColumnVector> createCudfColumnVector(const DataType &data_type,
                                                     C &&column) {
  return std::make_unique<T>(data_type, std::forward<C>(column));
}
#endif

std::unique_ptr<ColumnScalar>
createArrowColumnScalar(const DataType &data_type, size_t size,
                        std::shared_ptr<arrow::Scalar> scalar);

std::unique_ptr<ColumnVector>
createArrowColumnVector(const DataType &data_type,
                        std::shared_ptr<arrow::Array> array);

} // namespace cura::data
