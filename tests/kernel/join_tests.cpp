#include "cura/common/types.h"
#include "cura/kernel/hash_join.h"
#include "kernel_helper.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>

using cura::VoidKernelId;
using cura::VoidThreadId;
using cura::expression::ColumnIdx;
using cura::kernel::HashJoinBuild;
using cura::kernel::HashJoinProbe;
using cura::relational::BuildSide;
using cura::relational::JoinType;
using cura::test::data::makeDirectColumnVector;
using cura::test::data::makeDirectColumnVectorN;
using cura::test::data::makeFragment;
using cura::type::DataType;
using cura::type::Schema;

TEST(JoinTest, LeftJoin) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto build = std::make_shared<const HashJoinBuild>(
      0, Schema{DataType::int32Type()}, std::vector<ColumnIdx>{ColumnIdx{0}});
  auto probe = std::make_shared<const HashJoinProbe>(
      1, Schema{DataType::int32Type(), DataType::int32Type(true)},
      JoinType::LEFT, std::vector<ColumnIdx>{ColumnIdx{0}}, build,
      BuildSide::RIGHT);

  auto cv_build =
      makeDirectColumnVectorN<int32_t>(DataType::int32Type(), 3, 41);
  auto fragment_build = makeFragment(std::move(cv_build));

  build->push(ctx, VoidThreadId, VoidKernelId, fragment_build);
  build->concatenate(ctx);
  build->converge(ctx);

  auto cv_probe =
      makeDirectColumnVector<int32_t>(DataType::int32Type(), {22, 32, 42, 52});
  auto fragment_probe = makeFragment(std::move(cv_probe));
  auto res_fragment = probe->stream(ctx, 0, VoidKernelId, fragment_probe, 0);

  ASSERT_NE(res_fragment, nullptr);
  ASSERT_EQ(res_fragment->numColumns(), 2);
  ASSERT_EQ(res_fragment->size(), 4);

  auto res_cv_0 = res_fragment->column(0);
  auto res_cv_1 = res_fragment->column(1);
  ASSERT_EQ(res_cv_0->dataType().nullable, false);
  ASSERT_EQ(res_cv_1->dataType().nullable, true);
  auto expected_0 =
      makeDirectColumnVector<int32_t>(DataType::int32Type(), {22, 32, 42, 52});
  auto expected_1 = makeDirectColumnVector<int32_t>(
      DataType::int32Type(true), {0, 0, 42, 0}, {false, false, true, false});
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected_0, res_cv_0);
  CURA_TEST_EXPECT_COLUMNS_EQUAL_ORDERED(expected_1, res_cv_1);
}
