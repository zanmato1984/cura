#include "cura/common/types.h"
#include "cura/kernel/aggregate.h"
#include "cura/kernel/sources.h"
#include "kernel_helper.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>

using cura::VoidKernelId;
using cura::VoidThreadId;
using cura::data::Fragment;
using cura::execution::Context;
using cura::expression::AggregationOperator;
using cura::expression::ColumnIdx;
using cura::kernel::Aggregate;
using cura::kernel::HeapSource;
using cura::kernel::PhysicalAggregation;
using cura::test::data::makeDirectColumnScalar;
using cura::test::data::makeDirectColumnVector;
using cura::test::data::makeDirectColumnVectorN;
using cura::test::data::makeFragment;
using cura::type::DataType;
using cura::type::Schema;
using cura::type::TypeId;

std::shared_ptr<Aggregate> makeAggregateWithoutKey(AggregationOperator op,
                                                   DataType agg_in_type,
                                                   DataType agg_out_type) {
  return std::make_shared<Aggregate>(
      0, Schema{agg_in_type}, Schema{agg_out_type}, std::vector<ColumnIdx>{},
      std::vector<PhysicalAggregation>{{0, op, agg_out_type}});
}

std::shared_ptr<Aggregate> makeAggregate(DataType key_type,
                                         AggregationOperator op,
                                         DataType agg_in_type,
                                         DataType agg_out_type) {
  return std::make_shared<Aggregate>(
      0, Schema{key_type, agg_in_type}, Schema{key_type, agg_out_type},
      std::vector<ColumnIdx>{0},
      std::vector<PhysicalAggregation>{{1, op, agg_out_type}});
}

/// Use struct instead of a plain function to preserve life cycle of the used
/// heap, which holds the result fragment.
struct Aggregator {
  Option option;
  Context ctx = makeTrivialContext(option);

  ~Aggregator() = default;

  std::shared_ptr<const Fragment>
  aggregate(std::shared_ptr<const Aggregate> aggregate,
            std::shared_ptr<const Fragment> fragment) {
    aggregate->push(ctx, VoidThreadId, VoidKernelId, fragment);
    aggregate->concatenate(ctx);
    aggregate->converge(ctx);

    auto heap_source = std::make_shared<HeapSource>(1, aggregate);
    return heap_source->stream(ctx, VoidThreadId, VoidKernelId, nullptr, 1024);
  }
};

TEST(AggregateTest, Sum) {
  {
    Aggregator aggregator;
    auto aggregate =
        makeAggregate(DataType::int64Type(true), AggregationOperator::SUM,
                      DataType::int32Type(true), DataType::int64Type(true));

    auto cv0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 1, 2, 2, 0, 0},
        {true, true, true, true, false, false});
    auto cv1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 42, 0, 42},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv0), std::move(cv1));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 2);
    ASSERT_EQ(res->size(), 3);

    auto expected0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 2, 0}, {true, true, false});
    auto expected1 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {0, 84, 42}, {false, true, true});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::SUM,
                                             DataType::int32Type(true),
                                             DataType::int64Type(true));

    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 42, 0, 42},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected = makeDirectColumnVector<int64_t>(DataType::int64Type(true),
                                                    {126}, {true});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
}

TEST(AggregateTest, Min) {
  {
    Aggregator aggregator;
    auto aggregate =
        makeAggregate(DataType::int64Type(true), AggregationOperator::MIN,
                      DataType::int32Type(true), DataType::int32Type(true));

    auto cv0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 1, 2, 2, 0, 0},
        {true, true, true, true, false, false});
    auto cv1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv0), std::move(cv1));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 2);
    ASSERT_EQ(res->size(), 3);

    auto expected0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 2, 0}, {true, true, false});
    auto expected1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 42, 44}, {false, true, true});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::MIN,
                                             DataType::int32Type(true),
                                             DataType::int32Type(true));

    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected = makeDirectColumnVector<int32_t>(DataType::int32Type(true),
                                                    {42}, {true});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
}

TEST(AggregateTest, Max) {
  {
    Aggregator aggregator;
    auto aggregate =
        makeAggregate(DataType::int64Type(true), AggregationOperator::MAX,
                      DataType::int32Type(true), DataType::int64Type());

    auto cv0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 1, 2, 2, 0, 0},
        {true, true, true, true, false, false});
    auto cv1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {41, 0, 42, 43, 0, 44},
        {true, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv0), std::move(cv1));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 2);
    ASSERT_EQ(res->size(), 3);

    auto expected0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 2, 0}, {true, true, false});
    auto expected1 =
        makeDirectColumnVector<int64_t>(DataType::int64Type(), {41, 43, 44});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::MAX,
                                             DataType::int32Type(true),
                                             DataType::int32Type());

    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {41, 0, 42, 43, 0, 44},
        {true, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected =
        makeDirectColumnVector<int32_t>(DataType::int32Type(), {44});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
}

TEST(AggregateTest, CountValid) {
  {
    Aggregator aggregator;
    auto aggregate = makeAggregate(
        DataType::int64Type(true), AggregationOperator::COUNT_VALID,
        DataType::int32Type(true), DataType::int64Type());

    auto cv0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 1, 2, 2, 0, 0},
        {true, true, true, true, false, false});
    auto cv1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv0), std::move(cv1));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 2);
    ASSERT_EQ(res->size(), 3);

    auto expected0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 2, 0}, {true, true, false});
    auto expected1 =
        makeDirectColumnVector<int64_t>(DataType::int64Type(), {0, 2, 1});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::COUNT_VALID,
                                             DataType::int32Type(true),
                                             DataType::int64Type());

    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected = makeDirectColumnVector<int64_t>(DataType::int64Type(), {3});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
}

TEST(AggregateTest, CountAll) {
  {
    Aggregator aggregator;
    auto aggregate =
        makeAggregate(DataType::int64Type(true), AggregationOperator::COUNT_ALL,
                      DataType::int32Type(true), DataType::int64Type());

    auto cv0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 1, 2, 2, 0, 0},
        {true, true, true, true, false, false});
    auto cv1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv0), std::move(cv1));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 2);
    ASSERT_EQ(res->size(), 3);

    auto expected0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 2, 0}, {true, true, false});
    auto expected1 =
        makeDirectColumnVector<int64_t>(DataType::int64Type(), {2, 2, 2});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::COUNT_ALL,
                                             DataType::int32Type(true),
                                             DataType::int64Type());

    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected = makeDirectColumnVector<int64_t>(DataType::int64Type(), {6});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
}

TEST(AggregateTest, Mean) {
  {
    Aggregator aggregator;
    auto aggregate =
        makeAggregate(DataType::int64Type(true), AggregationOperator::MEAN,
                      DataType::int32Type(true), DataType::float64Type(true));

    auto cv0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 1, 2, 2, 0, 0},
        {true, true, true, true, false, false});
    auto cv1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv0), std::move(cv1));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 2);
    ASSERT_EQ(res->size(), 3);

    auto expected0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 2, 0}, {true, true, false});
    auto expected1 = makeDirectColumnVector<double>(
        DataType::float64Type(true), {0, 42.5, 44}, {false, true, true});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::MEAN,
                                             DataType::int32Type(true),
                                             DataType::float64Type(true));

    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected =
        makeDirectColumnVector<double>(DataType::float64Type(), {43});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
}

TEST(AggregateTest, CountLiteral) {
  Aggregator aggregator;
  auto aggregate = std::make_shared<Aggregate>(
      0,
      Schema{DataType::int32Type(), DataType::int32Type(),
             DataType::int32Type(true), DataType::int32Type(true)},
      Schema{DataType::int64Type(), DataType::int64Type(),
             DataType::int64Type(), DataType::int64Type()},
      std::vector<ColumnIdx>{},
      std::vector<PhysicalAggregation>{
          {0, AggregationOperator::COUNT_VALID, DataType::int64Type()},
          {0, AggregationOperator::COUNT_ALL, DataType::int64Type()},
          {2, AggregationOperator::COUNT_VALID, DataType::int64Type()},
          {2, AggregationOperator::COUNT_ALL, DataType::int64Type()}});

  auto c0 = makeDirectColumnScalar(DataType::int32Type(), 1, 7);
  auto c1 = makeDirectColumnVectorN(DataType::int32Type(), 7, 42);
  auto c2 = makeDirectColumnScalar(DataType::int32Type(true), 0, 7);
  auto c3 = makeDirectColumnVector<int32_t>(
      DataType::int32Type(true), {40, 41, 42, 43, 44, 46, 47},
      {false, false, true, true, false, true, false});
  auto fragment =
      makeFragment(std::move(c0), std::move(c1), std::move(c2), std::move(c3));
  auto res = aggregator.aggregate(aggregate, fragment);
  ASSERT_NE(res, nullptr);
  ASSERT_EQ(res->numColumns(), 4);
  ASSERT_EQ(res->size(), 1);

  auto seven = makeDirectColumnVector<int64_t>(DataType::int64Type(), {7});
  auto zero = makeDirectColumnVector<int64_t>(DataType::int64Type(), {0});
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(seven, res->column(0));
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(seven, res->column(1));
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(zero, res->column(2));
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(seven, res->column(3));
}

TEST(AggregateTest, NthElement) {
  {
    Aggregator aggregator;
    auto aggregate = makeAggregate(
        DataType::int64Type(true), AggregationOperator::NTH_ELEMENT,
        DataType::int32Type(true), DataType::int32Type(true));

    auto cv0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 1, 2, 2, 0, 0},
        {true, true, true, true, false, false});
    auto cv1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv0), std::move(cv1));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 2);
    ASSERT_EQ(res->size(), 3);

    auto expected0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 2, 0}, {true, true, false});
    auto expected1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 42, 0}, {false, true, false});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::NTH_ELEMENT,
                                             DataType::int32Type(true),
                                             DataType::int32Type(true));

    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected = makeDirectColumnVector<int32_t>(DataType::int32Type(true),
                                                    {0}, {false});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::NTH_ELEMENT,
                                             DataType::int32Type(true),
                                             DataType::int32Type(true));

    auto cv =
        makeDirectColumnVector<int32_t>(DataType::int32Type(true), {}, {});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected = makeDirectColumnVector<int32_t>(DataType::int32Type(true),
                                                    {0}, {false});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
}

#ifdef USE_CUDF
TEST(AggregateTest, NUnique) {
  {
    Aggregator aggregator;
    auto aggregate =
        makeAggregate(DataType::int64Type(true), AggregationOperator::NUNIQUE,
                      DataType::int32Type(true), DataType::int64Type());

    auto cv0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 1, 2, 2, 0, 0},
        {true, true, true, true, false, false});
    auto cv1 = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv0), std::move(cv1));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 2);
    ASSERT_EQ(res->size(), 3);

    auto expected0 = makeDirectColumnVector<int64_t>(
        DataType::int64Type(true), {1, 2, 0}, {true, true, false});
    auto expected1 =
        makeDirectColumnVector<int64_t>(DataType::int64Type(), {1, 2, 2});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  }
  {
    Aggregator aggregator;
    auto aggregate = makeAggregateWithoutKey(AggregationOperator::NUNIQUE,
                                             DataType::int32Type(true),
                                             DataType::int64Type());

    auto cv = makeDirectColumnVector<int32_t>(
        DataType::int32Type(true), {0, 0, 42, 43, 0, 44},
        {false, false, true, true, false, true});
    auto fragment = makeFragment(std::move(cv));
    auto res = aggregator.aggregate(aggregate, fragment);
    ASSERT_NE(res, nullptr);
    ASSERT_EQ(res->numColumns(), 1);
    ASSERT_EQ(res->size(), 1);

    auto expected = makeDirectColumnVector<int64_t>(DataType::int64Type(), {4});
    CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected, res->column(0));
  }
}
#endif