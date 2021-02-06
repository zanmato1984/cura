#include "cura/driver/option.h"
#include "cura/expression/binary_op.h"
#include "cura/expression/literal.h"
#include "cura/expression/ti_unary_op.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>
#include <numeric>

using cura::data::ColumnVector;
using cura::driver::Option;
using cura::execution::Context;
using cura::expression::BinaryOp;
using cura::expression::BinaryOperator;
using cura::expression::ColumnRef;
using cura::expression::Literal;
using cura::expression::TiUnaryOp;
using cura::test::data::makeDirectColumnVector;
using cura::test::data::makeDirectColumnVectorN;
using cura::test::data::makeFragment;
using cura::type::DataType;
using cura::type::TypeId;

TEST(ExpressionTest, VectorAddVector) {
  Option option;
  Context ctx(option);

  auto column_ref_0 =
      std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto column_ref_1 =
      std::make_shared<const ColumnRef>(1, DataType::int32Type());
  auto plus = std::make_shared<const BinaryOp>(
      BinaryOperator::ADD, column_ref_0, column_ref_1, DataType::int32Type());

  auto cv_0 = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 5);
  auto cv_1 =
      makeDirectColumnVector<int32_t>(DataType::int32Type(), {2, 4, 6, 8, 10});
  auto fragment = makeFragment(std::move(cv_0), std::move(cv_1));

  auto c_res = plus->evaluate(ctx, 0, *fragment);
  auto cv_res = std::dynamic_pointer_cast<const ColumnVector>(c_res);
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::int32Type());
  ASSERT_EQ(cv_res->size(), 5);

  auto expected =
      makeDirectColumnVector<int32_t>(DataType::int32Type(), {2, 5, 8, 11, 14});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}

TEST(ExpressionTest, VectorEqLiteralInt32) {
  Option option;
  Context ctx(option);

  auto column_ref = std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto literal = std::make_shared<const Literal>(TypeId::INT32, 42);
  auto eq = std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, column_ref,
                                             literal, DataType::bool8Type());

  auto cv_0 = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 5, 40);
  auto cv_1 =
      makeDirectColumnVector<int32_t>(DataType::int32Type(), {2, 4, 6, 8, 10});
  auto fragment = makeFragment(std::move(cv_0), std::move(cv_1));

  auto c_res = eq->evaluate(ctx, 0, *fragment);
  auto cv_res = std::dynamic_pointer_cast<const ColumnVector>(c_res);
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::bool8Type());
  ASSERT_EQ(cv_res->size(), 5);
  auto expected = makeDirectColumnVector<bool>(
      DataType::bool8Type(), {false, false, true, false, false});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}

TEST(ExpressionTest, VectorEqLiteralString) {
  Option option;
  Context ctx(option);

  auto column_ref =
      std::make_shared<const ColumnRef>(0, DataType::stringType());
  auto literal =
      std::make_shared<const Literal>(TypeId::STRING, std::string("test"));
  auto eq = std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, column_ref,
                                             literal, DataType::bool8Type());

  auto cv_0 = makeDirectColumnVector<std::string>(
      DataType::stringType(), {"abc", "", "test", "def", "yes"});
  auto cv_1 = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 5, 40);
  auto fragment = makeFragment(std::move(cv_0), std::move(cv_1));

  auto c_res = eq->evaluate(ctx, 0, *fragment);
  auto cv_res = std::dynamic_pointer_cast<const ColumnVector>(c_res);
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::bool8Type());
  ASSERT_EQ(cv_res->size(), 5);
  auto expected = makeDirectColumnVector<bool>(
      DataType::bool8Type(), {false, false, true, false, false});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}

TEST(ExpressionTest, VectorIntEqLiteralFloat64) {
  Option option;
  Context ctx(option);

  auto column_ref = std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto literal = std::make_shared<const Literal>(TypeId::FLOAT64, 42);
  auto eq = std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, column_ref,
                                             literal, DataType::bool8Type());

  auto cv = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 3, 41);
  auto fragment = makeFragment(std::move(cv));

  auto c_res = eq->evaluate(ctx, 0, *fragment);
  auto cv_res = std::dynamic_pointer_cast<const ColumnVector>(c_res);
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::bool8Type());
  ASSERT_EQ(cv_res->size(), 3);
  auto expected =
      makeDirectColumnVector<bool>(DataType::bool8Type(), {false, true, false});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}

TEST(ExpressionTest, VectorFloat32EqLiteralInt64) {
  Option option;
  Context ctx(option);

  auto column_ref =
      std::make_shared<const ColumnRef>(0, DataType::float32Type());
  auto literal = std::make_shared<const Literal>(TypeId::UINT64, uint64_t{42});
  auto eq = std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, column_ref,
                                             literal, DataType::bool8Type());

  auto cv = makeDirectColumnVector<float>(DataType::float32Type(),
                                          {12.3f, 42.00f, 42.42f});
  auto fragment = makeFragment(std::move(cv));

  auto c_res = eq->evaluate(ctx, 0, *fragment);
  auto cv_res = std::dynamic_pointer_cast<const ColumnVector>(c_res);
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::bool8Type());
  ASSERT_EQ(cv_res->size(), 3);
  auto expected =
      makeDirectColumnVector<bool>(DataType::bool8Type(), {false, true, false});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}

TEST(ExpressionTest, VectorAddVectorEqLiteral) {
  Option option;
  Context ctx(option);

  auto column_ref_0 =
      std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto column_ref_1 =
      std::make_shared<const ColumnRef>(1, DataType::int32Type());
  auto plus = std::make_shared<const BinaryOp>(
      BinaryOperator::ADD, column_ref_0, column_ref_1, DataType::int32Type());
  auto literal = std::make_shared<const Literal>(TypeId::INT32, 42);
  auto eq = std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, plus,
                                             literal, DataType::bool8Type());

  auto cv_0 = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 5, 40);
  auto cv_1 = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 5);
  auto fragment = makeFragment(std::move(cv_0), std::move(cv_1));

  auto c_res = eq->evaluate(ctx, 0, *fragment);
  auto cv_res = std::dynamic_pointer_cast<const ColumnVector>(c_res);
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::bool8Type());
  ASSERT_EQ(cv_res->size(), 5);
  auto expected = makeDirectColumnVector<bool>(
      DataType::bool8Type(), {false, true, false, false, false});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}

TEST(ExpressionTest, VectorEqLiteralAndLiteralEqVectorAddVector) {
  Option option;
  Context ctx(option);

  auto column_ref_0 =
      std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto column_ref_1 =
      std::make_shared<const ColumnRef>(1, DataType::float64Type());
  auto literal_0 = std::make_shared<const Literal>(TypeId::INT64, int64_t{42});
  auto literal_1 = std::make_shared<const Literal>(TypeId::FLOAT64, 42.42);
  auto eq_0 = std::make_shared<const BinaryOp>(
      BinaryOperator::EQUAL, column_ref_0, literal_0, DataType::bool8Type());
  auto plus = std::make_shared<const BinaryOp>(
      BinaryOperator::ADD, column_ref_0, column_ref_1, DataType::float64Type());
  auto eq_1 = std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, literal_1,
                                               plus, DataType::bool8Type());
  auto logical_and = std::make_shared<const BinaryOp>(
      BinaryOperator::LOGICAL_AND, eq_0, eq_1, DataType::bool8Type());

  auto cv_0 =
      makeDirectColumnVector<int32_t>(DataType::int32Type(), {42, 42, 43});
  auto cv_1 = makeDirectColumnVector<double>(DataType::float64Type(),
                                             {0.41, 0.42, 0.43});
  auto fragment = makeFragment(std::move(cv_0), std::move(cv_1));

  auto c_res = logical_and->evaluate(ctx, 0, *fragment);
  auto cv_res = std::dynamic_pointer_cast<const ColumnVector>(c_res);
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::bool8Type());
  ASSERT_EQ(cv_res->size(), 3);
  auto expected =
      makeDirectColumnVector<bool>(DataType::bool8Type(), {false, true, false});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}

// TODO: cudf doesn't support kleene logic so temporarily disable checking
// result.
TEST(ExpressionTest, NullableAnd) {
  Option option;
  Context ctx(option);

  auto column_ref_0 =
      std::make_shared<const ColumnRef>(0, DataType::bool8Type(true));
  auto column_ref_1 =
      std::make_shared<const ColumnRef>(1, DataType::bool8Type());
  auto column_ref_2 =
      std::make_shared<const ColumnRef>(2, DataType::bool8Type());

  auto cv_0 = makeDirectColumnVector<bool>(DataType::bool8Type(true),
                                           {true, false, true, false},
                                           {true, true, false, false});
  auto cv_1 = makeDirectColumnVector<bool>(DataType::bool8Type(),
                                           {true, true, true, true});
  auto cv_2 = makeDirectColumnVector<bool>(DataType::bool8Type(),
                                           {false, false, false, false});
  auto fragment =
      makeFragment(std::move(cv_0), std::move(cv_1), std::move(cv_2));

  {
    auto logical_and = std::make_shared<const BinaryOp>(
        BinaryOperator::LOGICAL_AND, column_ref_0, column_ref_1,
        DataType::bool8Type(true));

    auto c_res = logical_and->evaluate(ctx, 0, *fragment);
    auto cv_res = dynamic_cast<const ColumnVector *>(c_res.get());
    ASSERT_NE(cv_res, nullptr);
    ASSERT_EQ(cv_res->dataType(), DataType::bool8Type(true));
    ASSERT_EQ(cv_res->size(), 4);
    auto expected = makeDirectColumnVector<bool>(DataType::bool8Type(true),
                                                 {true, false, false, false},
                                                 {true, true, false, false});
#ifndef USE_CUDF
    CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
#endif
  }

  {
    auto logical_and = std::make_shared<const BinaryOp>(
        BinaryOperator::LOGICAL_AND, column_ref_2, column_ref_0,
        DataType::bool8Type(true));

    auto c_res = logical_and->evaluate(ctx, 0, *fragment);
    auto cv_res = dynamic_cast<const ColumnVector *>(c_res.get());
    ASSERT_NE(cv_res, nullptr);
    ASSERT_EQ(cv_res->dataType(), DataType::bool8Type(true));
    ASSERT_EQ(cv_res->size(), 4);
    auto expected = makeDirectColumnVector<bool>(DataType::bool8Type(true),
                                                 {false, false, false, false},
                                                 {true, true, true, true});
#ifndef USE_CUDF
    CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
#endif
  }
}

TEST(ExpressionTest, ChronoColumnAndLiteral) {
  Option option;
  Context ctx(option);

  auto literal = std::make_shared<const Literal>(TypeId::TIMESTAMP_DAYS, 42);
  auto column_ref =
      std::make_shared<const ColumnRef>(0, DataType::timestampDaysType());

  auto d0 = arrow::Date32Scalar(100);
  auto d1 = arrow::Date32Scalar(42);
  auto cv = makeDirectColumnVector<arrow::Date32Type::c_type>(
      DataType::timestampDaysType(), {d0.value, d1.value});
  auto fragment = makeFragment(std::move(cv));

  auto eq = std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, column_ref,
                                             literal, DataType::bool8Type());

  auto c_res = eq->evaluate(ctx, 0, *fragment);
  auto cv_res = dynamic_cast<const ColumnVector *>(c_res.get());
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::bool8Type());
  ASSERT_EQ(cv_res->size(), 2);
  auto expected =
      makeDirectColumnVector<bool>(DataType::bool8Type(), {false, true});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}

TEST(ExpressionTest, ExtractYearLiteral) {
  Option option;
  Context ctx(option);

  auto column_ref =
      std::make_shared<const ColumnRef>(0, DataType::int64Type(true));

  auto cv =
      makeDirectColumnVector<int64_t>(DataType::int64Type(true),
                                      {
                                          2234574864524509196, // 1984-11-07
                                          0,
                                          2241081774337687564 // 1990-07-22
                                      },
                                      {true, false, true});
  auto fragment = makeFragment(std::move(cv));

  auto ey =
      std::make_shared<const TiUnaryOp>(column_ref, DataType::int64Type(true),
                                        TiUnaryOp::UnaryOperator::EXTRACT_YEAR);

  auto c_res = ey->evaluate(ctx, 0, *fragment);
  auto cv_res = dynamic_cast<const ColumnVector *>(c_res.get());
  ASSERT_NE(cv_res, nullptr);
  ASSERT_EQ(cv_res->dataType(), DataType::int64Type(true));
  ASSERT_EQ(cv_res->size(), 3);
  auto expected = makeDirectColumnVector<int64_t>(
      DataType::int64Type(true), {1984, 0, 1990}, {true, false, true});
  CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, cv_res);
}
