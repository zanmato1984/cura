#include "cura/kernel/sort.h"
#include "cura/kernel/sources.h"
#include "kernel_helper.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>

using cura::VoidKernelId;
using cura::VoidThreadId;
using cura::kernel::HeapSource;
using cura::kernel::PhysicalSortInfo;
using cura::kernel::Sort;
using cura::relational::SortInfo;
using cura::test::data::assertColumnsEqual;
using cura::test::data::makeDirectColumnVector;
using cura::test::data::makeDirectColumnVectorN;
using cura::test::data::makeFragment;
using cura::type::DataType;
using cura::type::Schema;

TEST(SortTest, SimpleSort) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto sort = std::make_shared<const Sort>(
      0,
      Schema{DataType::int32Type(true), DataType::int32Type(),
             DataType::stringType(true)},
      std::vector<PhysicalSortInfo>{
          {{0, SortInfo::Order::ASCENDING, SortInfo::NullOrder::FIRST},
           {2, SortInfo::Order::DESCENDING, SortInfo::NullOrder::LAST}}});
  auto c0 = makeDirectColumnVector(
      DataType::int32Type(true), std::vector<std::int32_t>{42, 0, 42, 1, 0, 0},
      {true, true, true, false, false, true});
  auto c1 = makeDirectColumnVectorN(DataType::int32Type(), 6, 42);
  auto c2 = makeDirectColumnVector(
      DataType::stringType(true),
      std::vector<std::string>{"y", "a", "z", "i", "j", "b"},
      {false, true, true, true, false, true});
  auto fragment = makeFragment(std::move(c0), std::move(c1), std::move(c2));

  sort->push(ctx, VoidThreadId, VoidKernelId, fragment);
  sort->concatenate(ctx);
  sort->converge(ctx);

  auto heap_source = std::make_shared<HeapSource>(1, sort);
  auto res = heap_source->stream(ctx, 0, VoidKernelId, nullptr, 1024);

  ASSERT_NE(res, nullptr);
  ASSERT_EQ(res->numColumns(), 3);
  ASSERT_EQ(res->size(), 6);

  auto expected0 = makeDirectColumnVector<int32_t>(
      DataType::int32Type(true), {0, 0, 0, 0, 42, 42},
      {false, false, true, true, true, true});
  auto expected1 = makeDirectColumnVector<int32_t>(DataType::int32Type(),
                                                   {45, 46, 47, 43, 44, 42});
  auto expected2 = makeDirectColumnVector<std::string>(
      DataType::stringType(true), {"i", "", "b", "a", "z", ""},
      {true, false, true, true, true, false});
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected0, res->column(0));
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected1, res->column(1));
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected2, res->column(2));
}
