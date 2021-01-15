#include "helper.h"
#include "cura/data/column_scalar.h"
#include "cura/data/column_vector.h"

#ifdef USE_CUDF
#include <cudf/concatenate.hpp>
#endif

namespace cura::kernel::detail {

using cura::data::Column;
using cura::data::ColumnScalar;
using cura::data::ColumnVector;
using cura::data::createArrowColumnScalar;
using cura::data::createArrowColumnVector;

#ifdef USE_CUDF
using cura::data::ColumnVectorCudfColumn;
using cura::data::createCudfColumnScalar;
using cura::data::createCudfColumnVector;
using cura::type::DataType;
#endif

#ifdef USE_CUDF
std::shared_ptr<Fragment>
concatFragments(MemoryResource::Underlying *underlying, const Schema &schema,
                const std::vector<std::shared_ptr<const Fragment>> &fragments) {
  std::vector<bool> scalar_flags(fragments.front()->numColumns());
  std::vector<std::pair<std::shared_ptr<const ColumnScalar>, size_t>> scalars;
  std::vector<DataType> column_types;
  std::transform(
      fragments.front()->begin(), fragments.front()->end(),
      scalar_flags.begin(), [&](const auto &column) {
        if (auto cs = std::dynamic_pointer_cast<const ColumnScalar>(column);
            cs) {
          scalars.emplace_back(cs, 0);
          return true;
        } else {
          column_types.emplace_back(column->dataType());
          return false;
        }
      });
  std::vector<cudf::table_view> tables(fragments.size());
  std::transform(
      fragments.begin(), fragments.end(), tables.begin(),
      [&](const auto &fragment) {
        std::vector<cudf::column_view> columns;
        size_t scalar_idx = 0;
        for (const auto &column : *fragment) {
          if (auto cv = std::dynamic_pointer_cast<const ColumnVector>(column);
              cv) {
            columns.emplace_back(cv->cudf());
          } else if (auto cs =
                         std::dynamic_pointer_cast<const ColumnScalar>(column);
                     cs) {
            scalars[scalar_idx++].second += cs->size();
          } else {
            CURA_FAIL("Neither column vector nor column scalar");
          }
        }
        return cudf::table_view(columns);
      });
  auto columns = [&]() {
    if (tables.front().num_columns() > 0) {
      auto concat = cudf::concatenate(tables, underlying);
      return concat->release();
    } else {
      return std::vector<std::unique_ptr<cudf::column>>{};
    }
  }();
  std::vector<std::shared_ptr<const Column>> result;
  size_t scalar_idx = 0, column_idx = 0;
  for (const auto &scalar_flag : scalar_flags) {
    if (scalar_flag) {
      const auto &scalar = scalars[scalar_idx++];
      result.emplace_back(createCudfColumnScalar(
          scalar.first->dataType(), scalar.second, scalar.first->cudfPtr()));
    } else {
      auto &column_type = column_types[column_idx];
      auto &column = columns[column_idx];
      column_idx++;
      result.emplace_back(createCudfColumnVector<ColumnVectorCudfColumn>(
          std::move(column_type), std::move(column)));
    }
  };
  return std::make_shared<Fragment>(std::move(result));
}
#else
std::shared_ptr<Fragment>
concatFragments(MemoryResource::Underlying *underlying, const Schema &schema,
                const std::vector<std::shared_ptr<const Fragment>> &fragments) {
  std::vector<std::shared_ptr<const Column>> columns;
  for (size_t i_col = 0; i_col < fragments.front()->numColumns(); i_col++) {
    const auto &column = fragments.front()->column<Column>(i_col);
    if (auto cv = std::dynamic_pointer_cast<const ColumnVector>(column); cv) {
      std::vector<std::shared_ptr<arrow::Array>> arrays;
      const auto &data_type = fragments.front()->column(i_col)->dataType();
      for (const auto &fragment : fragments) {
        const auto &col = fragment->column(i_col);
        arrays.emplace_back(col->arrow());
      }
      const auto &concat =
          CURA_GET_ARROW_RESULT(arrow::Concatenate(arrays, underlying));
      auto concated = createArrowColumnVector(data_type, concat);
      columns.emplace_back(std::move(concated));
    } else if (auto cs = std::dynamic_pointer_cast<const ColumnScalar>(column);
               cs) {
      size_t size = 0;
      const auto &data_type =
          fragments.front()->column<ColumnScalar>(i_col)->dataType();
      for (const auto &fragment : fragments) {
        auto next_cs = fragment->column<ColumnScalar>(i_col);
        CURA_ASSERT(next_cs && next_cs->arrow()->Equals(cs->arrow()),
                    "Mismatched column scalar between fragments");
        size += next_cs->size();
      }
      auto concated = createArrowColumnScalar(data_type, size, cs->arrow());
      columns.emplace_back(std::move(concated));
    } else {
      CURA_FAIL("Neither column vector nor column scalar");
    }
  }
  return std::make_shared<Fragment>(std::move(columns));
}
#endif

} // namespace cura::kernel::detail
