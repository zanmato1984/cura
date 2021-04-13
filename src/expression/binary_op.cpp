#include "cura/expression/binary_op.h"
#include "cura/data/column_factories.h"
#include "cura/data/column_scalar.h"
#include "cura/data/column_vector.h"

#include <algorithm>
#include <arrow/compute/api.h>

#ifdef USE_CUDF
#include <cudf/binaryop.hpp>
#endif

namespace cura::expression {

using cura::data::ColumnScalar;
using cura::data::ColumnVector;
using cura::data::createArrowColumnVector;
using cura::type::TypeId;

#ifdef USE_CUDF
using cura::data::ColumnVectorCudfColumn;
using cura::data::createCudfColumnVector;
#endif

namespace detail {

#ifdef USE_CUDF
template <cudf::binary_operator op, typename LeftColumn, typename RightColumn>
std::shared_ptr<const Column> binaryOp(const Context &ctx, ThreadId thread_id,
                                       LeftColumn &&left, RightColumn &&right,
                                       const DataType &result_type) {
  auto res =
      cudf::binary_operation(left->cudf(), right->cudf(), op, result_type);
  return createCudfColumnVector<ColumnVectorCudfColumn>(result_type,
                                                        std::move(res));
}
#else
template <typename LeftColumn, typename RightColumn, typename Op>
struct BinaryOpHolder : public arrow::TypeVisitor {
  BinaryOpHolder(const Context &ctx_, ThreadId thread_id_, LeftColumn &&left_,
                 RightColumn &&right_, Op &&op_)
      : ctx(ctx_), thread_id(thread_id_), left(std::forward<LeftColumn>(left_)),
        right(std::forward<RightColumn>(right_)), op(std::forward<Op>(op_)) {}

  const Context &ctx;
  ThreadId thread_id;
  LeftColumn &&left;
  RightColumn &&right;
  Op &&op;

  std::shared_ptr<const Column> cv;
};

template <typename LeftArrowType, typename LeftColumn, typename RightColumn,
          typename Op>
struct RightTypeVisitor : public BinaryOpHolder<LeftColumn, RightColumn, Op> {
  using BinaryOpHolder<LeftColumn, RightColumn, Op>::BinaryOpHolder;

  template <typename RightArrowType> arrow::Status visit() {
    this->cv = this->op.template operator()<LeftArrowType, RightArrowType>(
        this->ctx, this->thread_id, std::forward<LeftColumn>(this->left),
        std::forward<RightColumn>(this->right));
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &type) override {
    return visit<arrow::BooleanType>();
  }

  arrow::Status Visit(const arrow::Int8Type &type) override {
    return visit<arrow::Int8Type>();
  }

  arrow::Status Visit(const arrow::Int16Type &type) override {
    return visit<arrow::Int16Type>();
  }

  arrow::Status Visit(const arrow::Int32Type &type) override {
    return visit<arrow::Int32Type>();
  }

  arrow::Status Visit(const arrow::Int64Type &type) override {
    return visit<arrow::Int64Type>();
  }

  arrow::Status Visit(const arrow::UInt8Type &type) override {
    return visit<arrow::UInt8Type>();
  }

  arrow::Status Visit(const arrow::UInt16Type &type) override {
    return visit<arrow::UInt16Type>();
  }

  arrow::Status Visit(const arrow::UInt32Type &type) override {
    return visit<arrow::UInt32Type>();
  }

  arrow::Status Visit(const arrow::UInt64Type &type) override {
    return visit<arrow::UInt64Type>();
  }

  arrow::Status Visit(const arrow::FloatType &type) override {
    return visit<arrow::FloatType>();
  }

  arrow::Status Visit(const arrow::DoubleType &type) override {
    return visit<arrow::DoubleType>();
  }

  arrow::Status Visit(const arrow::StringType &type) override {
    return visit<arrow::StringType>();
  }

  arrow::Status Visit(const arrow::Date32Type &type) override {
    return visit<arrow::Date32Type>();
  }

  arrow::Status Visit(const arrow::TimestampType &type) override {
    return visit<arrow::TimestampType>();
  }

  arrow::Status Visit(const arrow::DurationType &type) override {
    return visit<arrow::DurationType>();
  }
};

template <typename LeftColumn, typename RightColumn, typename Op>
struct LeftTypeVisitor : public BinaryOpHolder<LeftColumn, RightColumn, Op> {
  using BinaryOpHolder<LeftColumn, RightColumn, Op>::BinaryOpHolder;

  template <typename LeftArrowType> arrow::Status visit() {
    RightTypeVisitor<LeftArrowType, LeftColumn, RightColumn, Op> right_visitor(
        this->ctx, this->thread_id, std::forward<LeftColumn>(this->left),
        std::forward<RightColumn>(this->right), std::forward<Op>(this->op));
    CURA_ASSERT_ARROW_OK(
        this->right->dataType().arrow()->Accept(&right_visitor),
        "Dispatch right type for binary op failed");
    this->cv = std::move(right_visitor.cv);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::BooleanType &type) override {
    return visit<arrow::BooleanType>();
  }

  arrow::Status Visit(const arrow::Int8Type &type) override {
    return visit<arrow::Int8Type>();
  }

  arrow::Status Visit(const arrow::Int16Type &type) override {
    return visit<arrow::Int16Type>();
  }

  arrow::Status Visit(const arrow::Int32Type &type) override {
    return visit<arrow::Int32Type>();
  }

  arrow::Status Visit(const arrow::Int64Type &type) override {
    return visit<arrow::Int64Type>();
  }

  arrow::Status Visit(const arrow::UInt8Type &type) override {
    return visit<arrow::UInt8Type>();
  }

  arrow::Status Visit(const arrow::UInt16Type &type) override {
    return visit<arrow::UInt16Type>();
  }

  arrow::Status Visit(const arrow::UInt32Type &type) override {
    return visit<arrow::UInt32Type>();
  }

  arrow::Status Visit(const arrow::UInt64Type &type) override {
    return visit<arrow::UInt64Type>();
  }

  arrow::Status Visit(const arrow::FloatType &type) override {
    return visit<arrow::FloatType>();
  }

  arrow::Status Visit(const arrow::DoubleType &type) override {
    return visit<arrow::DoubleType>();
  }

  arrow::Status Visit(const arrow::StringType &type) override {
    return visit<arrow::StringType>();
  }

  arrow::Status Visit(const arrow::Date32Type &type) override {
    return visit<arrow::Date32Type>();
  }

  arrow::Status Visit(const arrow::TimestampType &type) override {
    return visit<arrow::TimestampType>();
  }

  arrow::Status Visit(const arrow::DurationType &type) override {
    return visit<arrow::DurationType>();
  }
};

template <typename LeftColumn, typename RightColumn, typename Op>
std::shared_ptr<const Column>
dispatchDualTypes(const Context &ctx, ThreadId thread_id, LeftColumn &&left,
                  RightColumn &&right, Op &&op) {
  detail::LeftTypeVisitor<LeftColumn, RightColumn, Op> left_visitor(
      ctx, thread_id, std::forward<LeftColumn>(left),
      std::forward<RightColumn>(right), std::forward<Op>(op));
  CURA_ASSERT_ARROW_OK(left->dataType().arrow()->Accept(&left_visitor),
                       "Dispatch for binary op failed");
  return std::move(left_visitor.cv);
}

// TODO: Now only support numeric types as other types, i.e. temporal, may need
// finer-grained type check and inference.
template <typename LeftColumn, typename RightColumn>
std::shared_ptr<const Column> add(const Context &ctx, ThreadId thread_id,
                                  LeftColumn &&left, RightColumn &&right,
                                  const DataType &result_type) {
  arrow::compute::ExecContext context(
      ctx.memory_resource->preConcatenate(thread_id));
  // Cast to result type as arrow doesn't support adding different types
  // regardless of implicit cast-able-ness.
  const auto &left_casted = CURA_GET_ARROW_RESULT(
      arrow::compute::Cast(left->arrow(), result_type.arrow(),
                           arrow::compute::CastOptions::Safe(), &context));
  const auto &right_casted = CURA_GET_ARROW_RESULT(
      arrow::compute::Cast(right->arrow(), result_type.arrow(),
                           arrow::compute::CastOptions::Safe(), &context));
  const auto &res = CURA_GET_ARROW_RESULT(
      arrow::compute::Add(left_casted, right_casted,
                          arrow::compute::ArithmeticOptions(), &context));
  CURA_ASSERT(res.kind() == arrow::Datum::ARRAY,
              "Binary op result must be an arrow array");
  return createArrowColumnVector(result_type, res.make_array());
}

template <typename LeftColumn, typename RightColumn> struct Equal {
  template <typename LeftArrowType, typename RightArrowType,
            std::enable_if_t<!arrow::is_number_type<LeftArrowType>::value ||
                             !arrow::is_number_type<RightArrowType>::value> * =
                nullptr>
  std::shared_ptr<const Column>
  operator()(const Context &ctx, ThreadId thread_id, LeftColumn &&left,
             RightColumn &&right) const {
    arrow::compute::ExecContext context(
        ctx.memory_resource->preConcatenate(thread_id));
    const auto &res = CURA_GET_ARROW_RESULT(arrow::compute::Compare(
        left->arrow(), right->arrow(),
        arrow::compute::CompareOptions(arrow::compute::CompareOperator::EQUAL),
        &context));
    CURA_ASSERT(res.kind() == arrow::Datum::ARRAY,
                "Binary op result must be an arrow array");
    return createArrowColumnVector(result_type, res.make_array());
  }

  template <typename LeftArrowType, typename RightArrowType,
            std::enable_if_t<arrow::is_number_type<LeftArrowType>::value &&
                             arrow::is_number_type<RightArrowType>::value> * =
                nullptr>
  std::shared_ptr<const Column>
  operator()(const Context &ctx, ThreadId thread_id, LeftColumn &&left,
             RightColumn &&right) const {
    using CommonCType = std::common_type_t<typename LeftArrowType::c_type,
                                           typename RightArrowType::c_type>;
    arrow::compute::ExecContext context(
        ctx.memory_resource->preConcatenate(thread_id));
    const auto &common_type = arrow::CTypeTraits<CommonCType>::type_singleton();
    const auto &left_casted = CURA_GET_ARROW_RESULT(
        arrow::compute::Cast(left->arrow(), common_type,
                             arrow::compute::CastOptions::Safe(), &context));
    const auto &right_casted = CURA_GET_ARROW_RESULT(
        arrow::compute::Cast(right->arrow(), common_type));
    const auto &res = CURA_GET_ARROW_RESULT(arrow::compute::Compare(
        left_casted, right_casted,
        arrow::compute::CompareOptions(arrow::compute::CompareOperator::EQUAL),
        &context));
    CURA_ASSERT(res.kind() == arrow::Datum::ARRAY,
                "Binary op result must be an arrow array");
    return createArrowColumnVector(result_type, res.make_array());
  }

  const DataType &result_type;
};

template <typename LeftColumn, typename RightColumn,
          arrow::compute::CompareOperator compareOp>
struct Compare {
  template <typename LeftArrowType, typename RightArrowType,
            std::enable_if_t<!arrow::is_number_type<LeftArrowType>::value ||
                             !arrow::is_number_type<RightArrowType>::value> * =
                nullptr>
  std::shared_ptr<const Column>
  operator()(const Context &ctx, ThreadId thread_id, LeftColumn &&left,
             RightColumn &&right) const {
    arrow::compute::ExecContext context(
        ctx.memory_resource->preConcatenate(thread_id));
    const auto &res = CURA_GET_ARROW_RESULT(arrow::compute::Compare(
        left->arrow(), right->arrow(),
        arrow::compute::CompareOptions(compareOp), &context));
    CURA_ASSERT(res.kind() == arrow::Datum::ARRAY,
                "Binary op result must be an arrow array");
    return createArrowColumnVector(result_type, res.make_array());
  }

  template <typename LeftArrowType, typename RightArrowType,
            std::enable_if_t<arrow::is_number_type<LeftArrowType>::value &&
                             arrow::is_number_type<RightArrowType>::value> * =
                nullptr>
  std::shared_ptr<const Column>
  operator()(const Context &ctx, ThreadId thread_id, LeftColumn &&left,
             RightColumn &&right) const {
    using CommonCType = std::common_type_t<typename LeftArrowType::c_type,
                                           typename RightArrowType::c_type>;
    arrow::compute::ExecContext context(
        ctx.memory_resource->preConcatenate(thread_id));
    const auto &common_type = arrow::CTypeTraits<CommonCType>::type_singleton();
    const auto &left_casted = CURA_GET_ARROW_RESULT(
        arrow::compute::Cast(left->arrow(), common_type,
                             arrow::compute::CastOptions::Safe(), &context));
    const auto &right_casted = CURA_GET_ARROW_RESULT(
        arrow::compute::Cast(right->arrow(), common_type));
    const auto &res = CURA_GET_ARROW_RESULT(arrow::compute::Compare(
        left_casted, right_casted, arrow::compute::CompareOptions(compareOp),
        &context));
    CURA_ASSERT(res.kind() == arrow::Datum::ARRAY,
                "Binary op result must be an arrow array");
    return createArrowColumnVector(result_type, res.make_array());
  }

  const DataType &result_type;
};

template <typename LeftColumn, typename RightColumn>
std::shared_ptr<const Column> eq(const Context &ctx, ThreadId thread_id,
                                 LeftColumn &&left, RightColumn &&right,
                                 const DataType &result_type) {
  Equal<LeftColumn, RightColumn> eq{result_type};
  return dispatchDualTypes(ctx, thread_id, std::forward<LeftColumn>(left),
                           std::forward<RightColumn>(right), eq);
}

template <typename LeftColumn, typename RightColumn,
          arrow::compute::CompareOperator compareOp>
std::shared_ptr<const Column> compare(const Context &ctx, ThreadId thread_id,
                                      LeftColumn &&left, RightColumn &&right,
                                      const DataType &result_type) {
  Compare<LeftColumn, RightColumn, compareOp> compare{result_type};
  return dispatchDualTypes(ctx, thread_id, std::forward<LeftColumn>(left),
                           std::forward<RightColumn>(right), compare);
}

template <typename LeftColumn, typename RightColumn>
std::shared_ptr<const Column> logicalAnd(const Context &ctx, ThreadId thread_id,
                                         LeftColumn &&left, RightColumn &&right,
                                         const DataType &result_type) {
  arrow::compute::ExecContext context(
      ctx.memory_resource->preConcatenate(thread_id));
  const auto &res = CURA_GET_ARROW_RESULT(
      arrow::compute::KleeneAnd(left->arrow(), right->arrow(), &context));
  CURA_ASSERT(res.kind() == arrow::Datum::ARRAY,
              "Binary op result must be an arrow array");
  return createArrowColumnVector(result_type, res.make_array());
}

template <typename LeftColumn, typename RightColumn>
std::shared_ptr<const Column> logicalOr(const Context &ctx, ThreadId thread_id,
                                        LeftColumn &&left, RightColumn &&right,
                                        const DataType &result_type) {
  arrow::compute::ExecContext context(
      ctx.memory_resource->preConcatenate(thread_id));
  const auto &res = CURA_GET_ARROW_RESULT(
      arrow::compute::KleeneOr(left->arrow(), right->arrow(), &context));
  CURA_ASSERT(res.kind() == arrow::Datum::ARRAY,
              "Binary op result must be an arrow array");
  return createArrowColumnVector(result_type, res.make_array());
}
#endif

template <typename LeftColumn, typename RightColumn>
std::shared_ptr<const Column>
dispatchBinaryOperator(const Context &ctx, ThreadId thread_id,
                       LeftColumn &&left, RightColumn &&right,
                       BinaryOperator binary_operator,
                       const DataType &result_type) {
  switch (binary_operator) {
#ifdef USE_CUDF
#define DO_BINARY_OP(OP, PRETTY)                                               \
  case BinaryOperator::OP:                                                     \
    return binaryOp<cudf::binary_operator::OP>(                                \
        ctx, thread_id, std::forward<LeftColumn>(left),                        \
        std::forward<RightColumn>(right), result_type);

    APPLY_FOR_BINARY_OPERATORS(DO_BINARY_OP)

#undef DO_BINARY_OP
#else
  case BinaryOperator::ADD:
    return add(ctx, thread_id, std::forward<LeftColumn>(left),
               std::forward<RightColumn>(right), result_type);
  case BinaryOperator::EQUAL:
    return compare<LeftColumn, RightColumn,
                   arrow::compute::CompareOperator::EQUAL>(
        ctx, thread_id, std::forward<LeftColumn>(left),
        std::forward<RightColumn>(right), result_type);
  case BinaryOperator::NOT_EQUAL:
    return compare<LeftColumn, RightColumn,
                   arrow::compute::CompareOperator::NOT_EQUAL>(
        ctx, thread_id, std::forward<LeftColumn>(left),
        std::forward<RightColumn>(right), result_type);
  case BinaryOperator::GREATER:
    return compare<LeftColumn, RightColumn,
                   arrow::compute::CompareOperator::GREATER>(
        ctx, thread_id, std::forward<LeftColumn>(left),
        std::forward<RightColumn>(right), result_type);
  case BinaryOperator::GREATER_EQUAL:
    return compare<LeftColumn, RightColumn,
                   arrow::compute::CompareOperator::GREATER_EQUAL>(
        ctx, thread_id, std::forward<LeftColumn>(left),
        std::forward<RightColumn>(right), result_type);
  case BinaryOperator::LESS:
    return compare<LeftColumn, RightColumn,
                   arrow::compute::CompareOperator::LESS>(
        ctx, thread_id, std::forward<LeftColumn>(left),
        std::forward<RightColumn>(right), result_type);
  case BinaryOperator::LESS_EQUAL:
    return compare<LeftColumn, RightColumn,
                   arrow::compute::CompareOperator::LESS_EQUAL>(
        ctx, thread_id, std::forward<LeftColumn>(left),
        std::forward<RightColumn>(right), result_type);
  case BinaryOperator::LOGICAL_AND:
    return logicalAnd(ctx, thread_id, std::forward<LeftColumn>(left),
                      std::forward<RightColumn>(right), result_type);
  case BinaryOperator::LOGICAL_OR:
    return logicalOr(ctx, thread_id, std::forward<LeftColumn>(left),
                     std::forward<RightColumn>(right), result_type);
#endif
  default:
    CURA_FAIL("Unimplemented");
  }
}

std::shared_ptr<const Column> dispatchBinaryOpColumns(
    const Context &ctx, ThreadId thread_id, std::shared_ptr<const Column> left,
    std::shared_ptr<const Column> right, BinaryOperator binary_operator,
    const DataType &result_type) {
  auto left_cv = std::dynamic_pointer_cast<const ColumnVector>(left);
  auto right_cv = std::dynamic_pointer_cast<const ColumnVector>(right);
  auto left_cs = std::dynamic_pointer_cast<const ColumnScalar>(left);
  auto right_cs = std::dynamic_pointer_cast<const ColumnScalar>(right);

  if (left_cv && right_cv) {
    return dispatchBinaryOperator(ctx, thread_id, left_cv, right_cv,
                                  binary_operator, result_type);
  } else if (left_cv && right_cs) {
    return dispatchBinaryOperator(ctx, thread_id, left_cv, right_cs,
                                  binary_operator, result_type);
  } else if (left_cs && right_cv) {
    return dispatchBinaryOperator(ctx, thread_id, left_cs, right_cv,
                                  binary_operator, result_type);
  } else if (left_cs && right_cs) {
    CURA_FAIL("Unimplemented");
  }

  CURA_FAIL("Shouldn't reach here");
}

std::shared_ptr<const Column>
evaluateBinaryOp(const Context &ctx, ThreadId thread_id,
                 std::shared_ptr<const Column> left,
                 std::shared_ptr<const Column> right,
                 BinaryOperator binary_operator, const DataType &result_type) {
  return dispatchBinaryOpColumns(ctx, thread_id, left, right, binary_operator,
                                 result_type);
}

} // namespace detail

std::shared_ptr<const Column>
BinaryOp::evaluate(const Context &ctx, ThreadId thread_id,
                   const Fragment &fragment) const {
  auto left = operands_[0]->evaluate(ctx, thread_id, fragment);
  auto right = operands_[1]->evaluate(ctx, thread_id, fragment);
  CURA_ASSERT(left, "Left column of binary op is null");
  CURA_ASSERT(right, "Right column of binary op is null");

  return detail::evaluateBinaryOp(ctx, thread_id, left, right, binary_operator,
                                  data_type);
}

} // namespace cura::expression
