#pragma once

#include "cura/common/errors.h"
#include "cura/data/column.h"
#include "cura/data/column_factories.h"

#include <arrow/api.h>

#ifdef USE_CUDF
#include <cudf/column/column.hpp>
#include <cudf/column/column_view.hpp>
#endif

namespace cura::data {

struct ColumnVector : public Column {
  using Column::Column;

  virtual std::unique_ptr<const ColumnVector> view(size_t offset,
                                                   size_t size) const = 0;

#ifdef USE_CUDF
  virtual cudf::column_view cudf() const = 0;
#endif

  virtual std::shared_ptr<arrow::Array> arrow() const = 0;
};

#ifdef USE_CUDF
struct ColumnVectorCudfColumnView;

struct ColumnVectorCudf : public ColumnVector {
  using ColumnVector::ColumnVector;

  size_t size() const override { return cudf().size(); }

  std::unique_ptr<const ColumnVector> view(size_t offset,
                                           size_t size) const override {
    auto full = cudf();
    if (offset + size > static_cast<size_t>(full.size())) {
      size = full.size() - offset;
    }
    std::vector<cudf::column_view> child_views(full.child_begin(),
                                               full.child_end());
    auto slice = cudf::column_view{
        full.type(),
        static_cast<cudf::size_type>(size),
        reinterpret_cast<const void *>(full.template data<char>()),
        full.null_mask(),
        full.null_count(),
        static_cast<cudf::size_type>(offset),
        child_views};
    return createCudfColumnVector<ColumnVectorCudfColumnView>(data_type, slice);
  }

  std::shared_ptr<arrow::Array> arrow() const override {
    CURA_FAIL("Calling cudf vector's arrow method");
  }
};

struct ColumnVectorCudfColumn : public ColumnVectorCudf {
  ColumnVectorCudfColumn(DataType data_type,
                         std::shared_ptr<cudf::column> column_)
      : ColumnVectorCudf(std::move(data_type)), column(column_) {
    CURA_ASSERT(column, "Underlying column couldn't be null");
    CURA_ASSERT(
        data_type == column->type(),
        "Mismatched data type between column vector and underlying column: " +
            data_type.toString() + " vs. " +
            DataType(column->type().id(), data_type.nullable).toString());
  }

  cudf::column_view cudf() const override { return column->view(); }

private:
  std::shared_ptr<cudf::column> column;
};

struct ColumnVectorCudfColumnView : public ColumnVectorCudf {
  ColumnVectorCudfColumnView(DataType data_type, cudf::column_view column_view_)
      : ColumnVectorCudf(std::move(data_type)),
        column_view(std::move(column_view_)) {
    CURA_ASSERT(
        data_type == column_view.type(),
        "Mismatched data type between column vector and underlying column");
  }

  cudf::column_view cudf() const override { return column_view; }

private:
  cudf::column_view column_view;
};
#endif

struct ColumnVectorArrow : public ColumnVector {
  using ColumnVector::ColumnVector;

  ColumnVectorArrow(DataType data_type_,
                    const std::shared_ptr<arrow::Array> array_)
      : ColumnVector(std::move(data_type_)), array(array_) {
    CURA_ASSERT(array, "Underlying column couldn't be null");
    CURA_ASSERT(
        array->type()->Equals(data_type.arrow()),
        "Mismatched data type between column vector and underlying column");
  }

  size_t size() const override { return array->length(); }

  std::unique_ptr<const ColumnVector> view(size_t offset,
                                           size_t size) const override {
    return std::make_unique<ColumnVectorArrow>(data_type,
                                               array->Slice(offset, size));
  }

#ifdef USE_CUDF
  cudf::column_view cudf() const override {
    CURA_FAIL("Calling arrow vector's cudf method");
  }
#endif

  std::shared_ptr<arrow::Array> arrow() const override { return array; }

private:
  const std::shared_ptr<arrow::Array> array;
};

} // namespace cura::data
