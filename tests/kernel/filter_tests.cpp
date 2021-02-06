#include "cura/common/types.h"
#include "cura/expression/binary_op.h"
#include "cura/expression/literal.h"
#include "cura/kernel/filter.h"
#include "kernel_helper.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>

using cura::VoidKernelId;
using cura::expression::BinaryOp;
using cura::expression::BinaryOperator;
using cura::expression::ColumnRef;
using cura::expression::Literal;
using cura::kernel::Filter;
using cura::test::data::makeDirectColumnVector;
using cura::test::data::makeDirectColumnVectorN;
using cura::test::data::makeFragment;
using cura::type::DataType;
using cura::type::Schema;
using cura::type::TypeId;

TEST(FilterTest, VectorAddVectorEqLiteral) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto column_ref_0 =
      std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto column_ref_1 =
      std::make_shared<const ColumnRef>(1, DataType::int32Type());
  auto plus = std::make_shared<const BinaryOp>(
      BinaryOperator::ADD, column_ref_0, column_ref_1, DataType::int32Type());
  auto literal = std::make_shared<const Literal>(TypeId::INT32, 42l);
  auto eq = std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, plus,
                                             literal, DataType::bool8Type());
  auto filter = std::make_shared<const Filter>(
      0, Schema{DataType::int32Type(), DataType::int32Type()}, eq);

  {
    auto cv_0 = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 5, 40);
    auto cv_1 = makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 5);
    auto fragment = makeFragment(std::move(cv_0), std::move(cv_1));

    auto res_fragment = filter->stream(ctx, 0, VoidKernelId, fragment, 0);

    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 2);
    ASSERT_EQ(res_fragment->size(), 1);

    auto res_cvv_0 = res_fragment->column(0);
    auto res_cvv_1 = res_fragment->column(1);
    auto expected_0 =
        makeDirectColumnVector<int32_t>(DataType::int32Type(), {41});
    auto expected_1 =
        makeDirectColumnVector<int32_t>(DataType::int32Type(), {1});
    CURA_TEST_EXPECT_COLUMNS_EQUAL(expected_0, res_cvv_0);
    CURA_TEST_EXPECT_COLUMNS_EQUAL(expected_1, res_cvv_1);
  }
}

TEST(FilterTest, VectorNullableEqLiteral) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto column_ref =
      std::make_shared<const ColumnRef>(0, DataType::int32Type(true));
  auto literal = std::make_shared<const Literal>(TypeId::INT32, 42);
  auto eq = std::make_shared<const BinaryOp>(
      BinaryOperator::EQUAL, column_ref, literal, DataType::bool8Type(true));
  auto filter =
      std::make_shared<const Filter>(0, Schema{DataType::int32Type(true)}, eq);

  {
    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {40, 41, 42, 42}, {true, true, false, true});
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = filter->stream(ctx, 0, VoidKernelId, fragment, 0);

    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 1);

    auto res_cv = res_fragment->column(0);
    auto expected = makeDirectColumnVector<int32_t>(DataType::int32Type(true),
                                                    {42}, {true});
    CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, res_cv);
  }
}

TEST(FilterTest, LiteralEqVectorNullable) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto literal = std::make_shared<const Literal>(TypeId::INT32, 42);
  auto column_ref =
      std::make_shared<const ColumnRef>(0, DataType::int32Type(true));
  auto eq = std::make_shared<const BinaryOp>(
      BinaryOperator::EQUAL, literal, column_ref, DataType::bool8Type(true));
  auto filter =
      std::make_shared<const Filter>(0, Schema{DataType::int32Type(true)}, eq);

  {
    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {40, 41, 42, 42}, {true, true, false, true});
    auto fragment = makeFragment(std::move(cv));

    auto res_fragment = filter->stream(ctx, 0, VoidKernelId, fragment, 0);

    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 1);

    auto res_cv = res_fragment->column(0);
    auto expected =
        makeDirectColumnVector<int32_t>(DataType::int32Type(), {42});
    CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, res_cv);
  }
}
