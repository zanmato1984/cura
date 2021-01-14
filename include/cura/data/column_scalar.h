#pragma once

#include "cura/common/errors.h"
#include "cura/data/column.h"

#include <optional>

#ifdef USE_CUDF
#include <cudf/scalar/scalar_factories.hpp>
#endif

namespace cura::data {

struct ColumnScalar : public Column {
  explicit ColumnScalar(DataType data_type, size_t size)
      : Column(std::move(data_type)), size_(size) {}

  size_t size() const override { return size_; }

#ifdef USE_CUDF
  virtual const cudf::scalar &cudf() const = 0;

  virtual std::shared_ptr<const cudf::scalar> cudfPtr() const = 0;
#endif

  virtual std::shared_ptr<arrow::Scalar> arrow() const = 0;

protected:
  size_t size_;
};

#ifdef USE_CUDF
struct ColumnScalarCudf : public ColumnScalar {
  ColumnScalarCudf(DataType data_type, size_t size,
                   std::shared_ptr<const cudf::scalar> scalar_)
      : ColumnScalar(std::move(data_type), size), scalar(scalar_) {}

  std::shared_ptr<arrow::Scalar> arrow() const override {
    CURA_FAIL("Calling cudf scalar's arrow method");
  }

  const cudf::scalar &cudf() const override { return *scalar; }

  std::shared_ptr<const cudf::scalar> cudfPtr() const override {
    return scalar;
  }

private:
  std::shared_ptr<const cudf::scalar> scalar;
};
#endif

struct ColumnScalarArrow : public ColumnScalar {
  ColumnScalarArrow(DataType data_type, size_t size,
                    std::shared_ptr<arrow::Scalar> scalar_)
      : ColumnScalar(std::move(data_type), size), scalar(scalar_) {}

  std::shared_ptr<arrow::Scalar> arrow() const override { return scalar; }

#ifdef USE_CUDF
  const cudf::scalar &cudf() const override {
    CURA_FAIL("Calling arrow scalar's cudf method");
  }

  std::shared_ptr<const cudf::scalar> cudfPtr() const override {
    CURA_FAIL("Calling arrow scalar's cudfPtr method");
  }
#endif

private:
  std::shared_ptr<arrow::Scalar> scalar;
};

} // namespace cura::data
