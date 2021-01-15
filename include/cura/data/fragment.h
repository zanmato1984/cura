#pragma once

#include "cura/expression/expressions.h"

#include <algorithm>

#ifdef USE_CUDF
#include <cudf/table/table.hpp>
#include <cudf/table/table_view.hpp>
#endif

namespace cura::data {

struct ColumnScalar;
struct ColumnVector;

using cura::expression::ColumnIdx;
using cura::type::Schema;

struct Fragment {
  explicit Fragment(std::vector<std::shared_ptr<const Column>> columns_);

#ifdef USE_CUDF
  explicit Fragment(const Schema &schema, std::unique_ptr<cudf::table> &&table);
#endif

  explicit Fragment(std::shared_ptr<arrow::RecordBatch> record_batch);

  size_t size() const;

  size_t numColumns() const { return columns.size(); }

  template <typename T = ColumnVector,
            std::enable_if_t<std::is_same_v<T, ColumnVector> ||
                             std::is_same_v<T, ColumnScalar>> * = nullptr>
  std::shared_ptr<const T> column(ColumnIdx idx) const {
    if (auto cc = std::dynamic_pointer_cast<const T>(columns[idx]); cc) {
      return cc;
    }
    CURA_FAIL("Required concrete column is invalid");
  }

  template <typename T, std::enable_if_t<std::is_same_v<T, Column>> * = nullptr>
  std::shared_ptr<const Column> column(ColumnIdx idx) const {
    return columns[idx];
  }

  auto begin() { return std::begin(columns); }
  auto begin() const { return std::cbegin(columns); }
  auto end() { return std::end(columns); }
  auto end() const { return std::cend(columns); }

#ifdef USE_CUDF
  cudf::table_view cudf() const;
#endif

  std::shared_ptr<arrow::RecordBatch> arrow() const;

private:
  std::vector<std::shared_ptr<const Column>> columns;
};

} // namespace cura::data
