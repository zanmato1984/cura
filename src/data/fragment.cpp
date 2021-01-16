#include "cura/data/fragment.h"
#include "cura/data/column_factories.h"
#include "cura/data/column_vector.h"

namespace cura::data {

#ifdef USE_CUDF
using cura::data::ColumnVectorCudfColumn;
using cura::data::createCudfColumnVector;
#endif

Fragment::Fragment(std::vector<std::shared_ptr<const Column>> columns_)
    : columns(std::move(columns_)) {
  CURA_ASSERT(!columns.empty(), "No column in fragment");
  CURA_ASSERT(std::all_of(columns.begin(), columns.end(),
                          [size = columns[0]->size()](const auto &column) {
                            return column->size() == size;
                          }),
              "Mismatched sizes between fragment columns");
}

#ifdef USE_CUDF
Fragment::Fragment(const Schema &schema, std::unique_ptr<cudf::table> &&table) {
  CURA_ASSERT(schema.size() == table->num_columns(),
              "Mismatched sizes between data types and cudf table columns " +
                  std::to_string(schema.size()) + ":" +
                  std::to_string(table->num_columns()));
  auto cols = table->release();
  for (size_t i = 0; i < schema.size(); i++) {
    columns.emplace_back(createCudfColumnVector<ColumnVectorCudfColumn>(
        schema[i], std::move(cols[i])));
  }
}
#endif

Fragment::Fragment(std::shared_ptr<arrow::RecordBatch> record_batch) {
  for (size_t i = 0; i < record_batch->num_columns(); i++) {
    const auto &field = record_batch->schema()->field(i);
    DataType data_type(field->type(), field->nullable());
    columns.emplace_back(
        createArrowColumnVector(std::move(data_type), record_batch->column(i)));
  }
}

size_t Fragment::size() const { return columns[0]->size(); }

#ifdef USE_CUDF
cudf::table_view Fragment::cudf() const {
  std::vector<cudf::column_view> cols;
  for (size_t i = 0; i < columns.size(); i++) {
    const auto &col = column(i);
    cols.emplace_back(col->cudf());
  }
  return cudf::table_view(cols);
}
#endif

std::shared_ptr<arrow::RecordBatch> Fragment::arrow() const {
  arrow::SchemaBuilder builder;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (size_t i = 0; i < columns.size(); i++) {
    const auto &col = column(i);
    const auto &data_type = col->dataType();
    auto field = std::make_shared<arrow::Field>("", data_type.arrow(),
                                                data_type.nullable);
    CURA_ASSERT_ARROW_OK(builder.AddField(field), "Add arrow field failed");
    arrays.emplace_back(col->arrow());
  }
  auto schema = CURA_GET_ARROW_RESULT(builder.Finish());
  return arrow::RecordBatch::Make(schema, size(), std::move(arrays));
}

} // namespace cura::data
