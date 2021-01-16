#pragma once

#include "cura/expression/expressions.h"

#include <arrow/api.h>
#include <arrow/visitor_inline.h>

#ifdef USE_CUDF
#include <cudf/scalar/scalar_factories.hpp>
#endif

namespace cura::expression {

using cura::type::TypeId;

#ifdef USE_CUDF
namespace detail {

template <typename T, typename Enable = void> struct CreateScalar {
  template <typename Type,
            std::enable_if_t<!cudf::is_chrono<Type>() &&
                             std::is_convertible_v<Type, std::decay_t<T>>> * =
                nullptr>
  std::unique_ptr<cudf::scalar> operator()(T &&value) const {
    return cudf::make_fixed_width_scalar(
        static_cast<Type>(std::forward<T>(value)));
  }

  template <typename Type,
            std::enable_if_t<!cudf::is_chrono<Type>() &&
                             !std::is_convertible_v<Type, std::decay_t<T>>> * =
                nullptr>
  std::unique_ptr<cudf::scalar> operator()(T &&value) const {
    CURA_FAIL("Invalid numeric scalar construction");
  }

  template <typename Type,
            std::enable_if_t<cudf::is_timestamp<Type>() &&
                             std::is_constructible_v<typename Type::duration,
                                                     T>> * = nullptr>
  std::unique_ptr<cudf::scalar> operator()(T &&value) const {
    return std::make_unique<cudf::timestamp_scalar<Type>>(
        Type(typename Type::duration(std::forward<T>(value))));
  }

  template <typename Type,
            std::enable_if_t<cudf::is_timestamp<Type>() &&
                             !std::is_constructible_v<typename Type::duration,
                                                      T>> * = nullptr>
  std::unique_ptr<cudf::scalar> operator()(T &&value) const {
    CURA_FAIL("Invalid timestamp scalar construction");
  }

  template <typename Type,
            std::enable_if_t<cudf::is_duration<Type>() &&
                             std::is_constructible_v<typename Type::duration,
                                                     T>> * = nullptr>
  std::unique_ptr<cudf::scalar> operator()(T &&value) const {
    return std::make_unique<cudf::duration_scalar<Type>>(
        typename Type::duration(std::forward<T>(value)));
  }

  template <typename Type,
            std::enable_if_t<cudf::is_duration<Type>() &&
                             !std::is_constructible_v<typename Type::duration,
                                                      T>> * = nullptr>
  std::unique_ptr<cudf::scalar> operator()(T &&value) const {
    CURA_FAIL("Invalid duration scalar construction");
  }
};

template <typename T>
struct CreateScalar<
    T, std::enable_if_t<std::is_same_v<std::decay_t<T>, std::string>>> {
  template <typename Type>
  std::unique_ptr<cudf::scalar> operator()(T &&value) const {
    return cudf::make_string_scalar(std::forward<T>(value));
  }
};

template <typename T, typename Enable = void> struct GetValue {
  template <typename Type,
            std::enable_if_t<cudf::is_fixed_width<Type>()> * = nullptr>
  T operator()(const cudf::scalar &scalar) const {
    using ScalarType = cudf::scalar_type_t<T>;
    auto p = static_cast<const ScalarType *>(&scalar);
    return static_cast<T>(p->value());
  }

  template <typename Type,
            std::enable_if_t<!cudf::is_fixed_width<Type>()> * = nullptr>
  T operator()(const cudf::scalar &scalar) const {
    CURA_FAIL("Invalid scalar get value");
  }
};

template <typename T>
struct GetValue<T, std::enable_if_t<std::is_constructible_v<T, std::string>>> {
  template <typename Type> T operator()(const cudf::scalar &scalar) const {
    auto p = static_cast<const cudf::string_scalar *>(&scalar);
    return p->to_string();
  }
};

} // namespace detail
#else
namespace detail {

template <typename T, std::enable_if_t<!std::is_constructible<
                          std::decay_t<T>, std::string>::value> * = nullptr>
inline std::shared_ptr<arrow::Scalar>
createScalar(std::shared_ptr<arrow::DataType> data_type, T &&value) {
  return CURA_GET_ARROW_RESULT(
      arrow::MakeScalar(data_type, std::forward<T>(value)));
}

template <typename T, std::enable_if_t<std::is_constructible<
                          std::decay_t<T>, std::string>::value> * = nullptr>
inline std::shared_ptr<arrow::Scalar>
createScalar(std::shared_ptr<arrow::DataType> data_type, T &&value) {
  return arrow::MakeScalar(value);
}

template <typename T, typename Enable = void> struct BaseScalarVisitor {
  template <typename ScalarType,
            std::enable_if_t<!std::is_same_v<arrow::StringScalar, ScalarType>>
                * = nullptr>
  arrow::Status visit(const ScalarType &scalar) {
    value = scalar.value;
    return arrow::Status::OK();
  }

  template <typename ScalarType,
            std::enable_if_t<std::is_same_v<arrow::StringScalar, ScalarType>>
                * = nullptr>
  arrow::Status visit(const ScalarType &scalar) {
    value = scalar.value->ToString();
    return arrow::Status::OK();
  }

  T value;
};

template <typename T, typename Enable = void> struct GetValueScalarVisitor {
  template <typename ScalarType,
            std::enable_if_t<std::is_same_v<T, typename ScalarType::ValueType>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    value = scalar.value;
    return arrow::Status::OK();
  }

  template <typename ScalarType,
            std::enable_if_t<std::is_same_v<ScalarType, arrow::NullScalar> ||
                             std::is_same_v<ScalarType, arrow::ExtensionScalar>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    return arrow::Status::NotImplemented(
        "GetValueScalarVisitor not implemented for " + scalar.ToString());
  }

  template <typename ScalarType,
            std::enable_if_t<!std::is_same_v<T, typename ScalarType::ValueType>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    return arrow::Status::NotImplemented(
        "GetValueScalarVisitor not implemented for " + scalar.ToString());
  }

  T value;
};

template <typename T>
struct GetValueScalarVisitor<
    T, std::enable_if_t<std::is_constructible_v<T, std::string>>> {
  template <typename ScalarType,
            std::enable_if_t<std::is_same_v<arrow::StringScalar, ScalarType>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    value = scalar.value->ToString();
    return arrow::Status::OK();
  }

  template <typename ScalarType,
            std::enable_if_t<!std::is_same_v<arrow::StringScalar, ScalarType>>
                * = nullptr>
  arrow::Status Visit(const ScalarType &scalar) {
    return arrow::Status::NotImplemented(
        "GetValueScalarVisitor not implemented for " + scalar.ToString());
  }

  std::string value;
};

} // namespace detail
#endif

struct Literal : public Expression {
#ifdef USE_CUDF
  explicit Literal(TypeId type_id)
      : data_type(type_id, true),
        scalar(cudf::make_default_constructed_scalar(data_type)) {
    scalar->set_valid(false);
  }
#else
  explicit Literal(TypeId type_id)
      : data_type(type_id, true),
        scalar(arrow::MakeNullScalar(data_type.arrow())) {}
#endif

  template <typename T>
  explicit Literal(TypeId type_id, T &&value) : data_type(type_id, false) {
#ifdef USE_CUDF
    scalar = cudf::type_dispatcher(data_type, detail::CreateScalar<T>{},
                                   std::forward<T>(value));
    CURA_ASSERT(data_type == scalar->type(),
                "Mismatched cura and cudf data types " + data_type.toString() +
                    " vs " + DataType(scalar->type().id(), false).toString());
#else
    scalar = detail::createScalar(data_type.arrow(), std::forward<T>(value));
    CURA_ASSERT(data_type.arrow()->Equals(scalar->type),
                "Mismatched cura and arrow data types " + data_type.toString() +
                    " vs " + DataType(scalar->type, false).toString());
#endif
  }

  const DataType &dataType() const override { return data_type; }

  std::shared_ptr<const Column>
  evaluate(const Context &ctx, ThreadId thread_id,
           const Fragment &fragment) const override;

  std::string name() const override { return "Literal"; }

  std::string toString() const override;

  template <typename T> T value() const {
#ifdef USE_CUDF
    return cudf::type_dispatcher(scalar->type(), detail::GetValue<T>{},
                                 *scalar);
#else
    detail::GetValueScalarVisitor<T> visitor;
    CURA_ASSERT_ARROW_OK(arrow::VisitScalarInline(*scalar, &visitor),
                         "Get arrow value from literal failed")
    return visitor.value;
#endif
  }

#ifdef USE_CUDF
  std::shared_ptr<const cudf::scalar> cudf() const { return scalar; }
#else
  std::shared_ptr<arrow::Scalar> arrow() const { return scalar; }
#endif

private:
  DataType data_type;
#ifdef USE_CUDF
  std::shared_ptr<cudf::scalar> scalar;
#else
  std::shared_ptr<arrow::Scalar> scalar;
#endif
};

} // namespace cura::expression
