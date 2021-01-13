#include "cura/data/fragment.h"
#include "cura/data/column_factories.h"
#include "cura/data/column_vector.h"

namespace cura::data {

Fragment::Fragment(std::vector<std::shared_ptr<const Column>> columns_)
    : columns(std::move(columns_)) {
  CURA_ASSERT(!columns.empty(), "No column in fragment");
  CURA_ASSERT(std::all_of(columns.begin(), columns.end(),
                          [size = columns[0]->size()](const auto &column) {
                            return column->size() == size;
                          }),
              "Mismatched sizes between fragment columns");
}

Fragment::Fragment(std::shared_ptr<arrow::RecordBatch> record_batch) {
  for (size_t i = 0; i < record_batch->num_columns(); i++) {
    const auto &field = record_batch->schema()->field(i);
    DataType data_type(field->type(), field->nullable());
    columns.emplace_back(
        createArrowColumnVector(std::move(data_type), record_batch->column(i)));
  }
}

size_t Fragment::size() const { return columns[0]->size(); }

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
