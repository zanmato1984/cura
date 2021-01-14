#include "cura/type/data_type.h"
#include "cura/common/errors.h"

#include <memory>

namespace cura::type {

namespace detail {

#ifdef USE_CUDF
constexpr TypeId fromCudfType(cudf::type_id type_id) {
#define CASE_FROM_CUDF(TYPE)                                                   \
  case cudf::type_id::TYPE:                                                    \
    return TypeId::TYPE;

  switch (type_id) {
    APPLY_FOR_TYPE_IDS(CASE_FROM_CUDF);
  default:
    return TypeId::EMPTY;
  }

#undef CASE_FROM_CUDF
}

constexpr cudf::type_id toCudfType(TypeId type_id) {
#define CASE_TO_CUDF(TYPE)                                                     \
  case TypeId::TYPE:                                                           \
    return cudf::type_id::TYPE;

  switch (type_id) {
    APPLY_FOR_TYPE_IDS(CASE_TO_CUDF);
  default:
    return cudf::type_id::EMPTY;
  }

#undef CASE_TO_CUDF
}
#endif

struct DataTypeTypeVisitor : public arrow::TypeVisitor {
  arrow::Status Visit(const arrow::NullType &type) override {
    type_id = TypeId::EMPTY;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &type) override {
    type_id = TypeId::BOOL8;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int8Type &type) override {
    type_id = TypeId::INT8;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int16Type &type) override {
    type_id = TypeId::INT16;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int32Type &type) override {
    type_id = TypeId::INT32;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Int64Type &type) override {
    type_id = TypeId::INT64;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt8Type &type) override {
    type_id = TypeId::UINT8;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt16Type &type) override {
    type_id = TypeId::UINT16;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt32Type &type) override {
    type_id = TypeId::UINT32;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::UInt64Type &type) override {
    type_id = TypeId::UINT64;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::FloatType &type) override {
    type_id = TypeId::FLOAT32;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DoubleType &type) override {
    type_id = TypeId::FLOAT64;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::StringType &type) override {
    type_id = TypeId::STRING;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::Date32Type &type) override {
    type_id = TypeId::TIMESTAMP_DAYS;
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::TimestampType &type) override {
    switch (type.unit()) {
    case arrow::TimeUnit::SECOND:
      type_id = TypeId::TIMESTAMP_SECONDS;
      break;
    case arrow::TimeUnit::MILLI:
      type_id = TypeId::TIMESTAMP_MILLISECONDS;
      break;
    case arrow::TimeUnit::MICRO:
      type_id = TypeId::TIMESTAMP_MICROSECONDS;
      break;
    case arrow::TimeUnit::NANO:
      type_id = TypeId::TIMESTAMP_NANOSECONDS;
      break;
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DurationType &type) override {
    switch (type.unit()) {
    case arrow::TimeUnit::SECOND:
      type_id = TypeId::DURATION_SECONDS;
      break;
    case arrow::TimeUnit::MILLI:
      type_id = TypeId::DURATION_MILLISECONDS;
      break;
    case arrow::TimeUnit::MICRO:
      type_id = TypeId::DURATION_MICROSECONDS;
      break;
    case arrow::TimeUnit::NANO:
      type_id = TypeId::DURATION_NANOSECONDS;
      break;
    }
    return arrow::Status::OK();
  }

  TypeId type_id;
};

TypeId fromArrowType(std::shared_ptr<arrow::DataType> data_type) {
  DataTypeTypeVisitor visitor;
  CURA_ASSERT_ARROW_OK(data_type->Accept(&visitor),
                       "Get type ID from arrow data type failed");
  return visitor.type_id;
}

std::shared_ptr<arrow::DataType> toArrowType(TypeId type_id) {
  switch (type_id) {
  case TypeId::EMPTY:
    return std::make_shared<arrow::NullType>();
  case TypeId::BOOL8:
    return std::make_shared<arrow::BooleanType>();
  case TypeId::INT8:
    return std::make_shared<arrow::Int8Type>();
  case TypeId::INT16:
    return std::make_shared<arrow::Int16Type>();
  case TypeId::INT32:
    return std::make_shared<arrow::Int32Type>();
  case TypeId::INT64:
    return std::make_shared<arrow::Int64Type>();
  case TypeId::UINT8:
    return std::make_shared<arrow::UInt8Type>();
  case TypeId::UINT16:
    return std::make_shared<arrow::UInt16Type>();
  case TypeId::UINT32:
    return std::make_shared<arrow::UInt32Type>();
  case TypeId::UINT64:
    return std::make_shared<arrow::UInt64Type>();
  case TypeId::FLOAT32:
    return std::make_shared<arrow::FloatType>();
  case TypeId::FLOAT64:
    return std::make_shared<arrow::DoubleType>();
  case TypeId::STRING:
    return std::make_shared<arrow::StringType>();
  case TypeId::TIMESTAMP_DAYS:
    return std::make_shared<arrow::Date32Type>();
  case TypeId::TIMESTAMP_SECONDS:
    return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::SECOND);
  case TypeId::TIMESTAMP_MILLISECONDS:
    return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MILLI);
  case TypeId::TIMESTAMP_MICROSECONDS:
    return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO);
  case TypeId::TIMESTAMP_NANOSECONDS:
    return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO);
  case TypeId::DURATION_SECONDS:
    return std::make_shared<arrow::DurationType>(arrow::TimeUnit::SECOND);
  case TypeId::DURATION_MILLISECONDS:
    return std::make_shared<arrow::DurationType>(arrow::TimeUnit::MILLI);
  case TypeId::DURATION_MICROSECONDS:
    return std::make_shared<arrow::DurationType>(arrow::TimeUnit::MICRO);
  case TypeId::DURATION_NANOSECONDS:
    return std::make_shared<arrow::DurationType>(arrow::TimeUnit::NANO);
  default:
    return std::make_shared<arrow::NullType>();
  }
}

} // namespace detail

#ifdef USE_CUDF
DataType::DataType(cudf::type_id type_id_, bool nullable_)
    : type_id(detail::fromCudfType(type_id_)), nullable(nullable_) {}
#endif

DataType::DataType(std::shared_ptr<arrow::DataType> data_type, bool nullable_)
    : type_id(detail::fromArrowType(data_type)), nullable(nullable_) {}

std::string DataType::toString() const {
  return typeIdToString(type_id) + (nullable ? "(NULLABLE)" : "");
}

bool DataType::operator==(const DataType &other) const {
  return type_id == other.type_id && nullable == other.nullable;
}

#ifdef USE_CUDF
DataType::operator cudf::data_type() const {
  return cudf::data_type{detail::toCudfType(type_id)};
}
#endif

std::shared_ptr<arrow::DataType> DataType::arrow() const {
  return detail::toArrowType(type_id);
}

} // namespace cura::type
