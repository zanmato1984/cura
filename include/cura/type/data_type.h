#pragma once

#include "cura/common/errors.h"

#include <arrow/api.h>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace cura::type {

#define APPLY_FOR_TYPE_IDS(ACTION)                                             \
  ACTION(EMPTY)                                                                \
  ACTION(BOOL8)                                                                \
  ACTION(INT8)                                                                 \
  ACTION(INT16)                                                                \
  ACTION(INT32)                                                                \
  ACTION(INT64)                                                                \
  ACTION(UINT8)                                                                \
  ACTION(UINT16)                                                               \
  ACTION(UINT32)                                                               \
  ACTION(UINT64)                                                               \
  ACTION(FLOAT32)                                                              \
  ACTION(FLOAT64)                                                              \
  ACTION(STRING)                                                               \
  ACTION(TIMESTAMP_DAYS)                                                       \
  ACTION(TIMESTAMP_SECONDS)                                                    \
  ACTION(TIMESTAMP_MILLISECONDS)                                               \
  ACTION(TIMESTAMP_MICROSECONDS)                                               \
  ACTION(TIMESTAMP_NANOSECONDS)                                                \
  ACTION(DURATION_SECONDS)                                                     \
  ACTION(DURATION_MILLISECONDS)                                                \
  ACTION(DURATION_MICROSECONDS)                                                \
  ACTION(DURATION_NANOSECONDS)
// ACTION(DURATION_DAYS)
// ACTION(DICTIONARY32)
// ACTION(LIST)
// ACTION(DECIMAL32)
// ACTION(DECIMAL64)
// ACTION(STRUCT)

#define DEF_TYPE_ID_ENUM(TYPE) TYPE,

enum class TypeId : int32_t { APPLY_FOR_TYPE_IDS(DEF_TYPE_ID_ENUM) };

#undef DEF_TYPE_ID_ENUM

inline std::string typeIdToString(TypeId type_id) {
#define TYPE_ID_CASE(TYPE)                                                     \
  case TypeId::TYPE:                                                           \
    return CURA_STRINGIFY(TYPE);

  switch (type_id) {
    APPLY_FOR_TYPE_IDS(TYPE_ID_CASE);
  default:
    CURA_FAIL("Unknown type id " +
              std::to_string(static_cast<int32_t>(type_id)));
  }

#undef TYPE_ID_CASE
}

inline TypeId typeIdFromString(const std::string &s) {
#define TYPE_ID_CASE(TYPE)                                                     \
  if (s == CURA_STRINGIFY(TYPE)) {                                             \
    return TypeId::TYPE;                                                       \
  }

  APPLY_FOR_TYPE_IDS(TYPE_ID_CASE)

  CURA_FAIL("Invalid type id: " + s);

#undef TYPE_ID_CASE
}

struct DataType {
  TypeId type_id;
  bool nullable;

  constexpr explicit DataType(TypeId type_id_ = TypeId::EMPTY,
                              bool nullable_ = false)
      : type_id(type_id_), nullable(nullable_) {}

  explicit DataType(std::shared_ptr<arrow::DataType> data_type,
                    bool nullable_ = false);

  std::string toString() const;

  bool operator==(const DataType &other) const;

  std::shared_ptr<arrow::DataType> arrow() const;

  static DataType bool8Type(bool nullable = false) {
    return DataType(TypeId::BOOL8, nullable);
  }

  static DataType int8Type(bool nullable = false) {
    return DataType(TypeId::INT8, nullable);
  }

  static DataType int16Type(bool nullable = false) {
    return DataType(TypeId::INT16, nullable);
  }

  static DataType int32Type(bool nullable = false) {
    return DataType(TypeId::INT32, nullable);
  }

  static DataType int64Type(bool nullable = false) {
    return DataType(TypeId::INT64, nullable);
  }

  static DataType uint8Type(bool nullable = false) {
    return DataType(TypeId::UINT8, nullable);
  }

  static DataType uint16Type(bool nullable = false) {
    return DataType(TypeId::UINT16, nullable);
  }

  static DataType uint32Type(bool nullable = false) {
    return DataType(TypeId::UINT32, nullable);
  }

  static DataType uint64Type(bool nullable = false) {
    return DataType(TypeId::UINT64, nullable);
  }

  static DataType float32Type(bool nullable = false) {
    return DataType(TypeId::FLOAT32, nullable);
  }

  static DataType float64Type(bool nullable = false) {
    return DataType(TypeId::FLOAT64, nullable);
  }

  static DataType stringType(bool nullable = false) {
    return DataType(TypeId::STRING, nullable);
  }

  static DataType timestampDaysType(bool nullable = false) {
    return DataType(TypeId::TIMESTAMP_DAYS, nullable);
  }

  static DataType timestampSecondsType(bool nullable = false) {
    return DataType(TypeId::TIMESTAMP_SECONDS, nullable);
  }

  static DataType timestampMillisecondsType(bool nullable = false) {
    return DataType(TypeId::TIMESTAMP_MILLISECONDS, nullable);
  }

  static DataType timestampMicorsecondsType(bool nullable = false) {
    return DataType(TypeId::TIMESTAMP_MICROSECONDS, nullable);
  }

  static DataType timestampNanosecondsType(bool nullable = false) {
    return DataType(TypeId::TIMESTAMP_NANOSECONDS, nullable);
  }

  static DataType durationSecondsType(bool nullable = false) {
    return DataType(TypeId::DURATION_SECONDS, nullable);
  }

  static DataType durationMillisecondsType(bool nullable = false) {
    return DataType(TypeId::DURATION_MILLISECONDS, nullable);
  }

  static DataType durationMicorsecondsType(bool nullable = false) {
    return DataType(TypeId::DURATION_MICROSECONDS, nullable);
  }

  static DataType durationNanosecondsType(bool nullable = false) {
    return DataType(TypeId::DURATION_NANOSECONDS, nullable);
  }
};

using Schema = std::vector<DataType>;

} // namespace cura::type
