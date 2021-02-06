#include "cura/common/types.h"
#include "cura/expression/binary_op.h"
#include "cura/kernel/project.h"
#include "kernel_helper.h"
#include "utilities/data_helper.h"

#include <gtest/gtest.h>

using cura::VoidKernelId;
using cura::expression::BinaryOp;
using cura::expression::BinaryOperator;
using cura::expression::ColumnRef;
using cura::expression::Expression;
using cura::kernel::Project;
using cura::test::data::makeDirectColumnVector;
using cura::test::data::makeDirectColumnVectorN;
using cura::test::data::makeFragment;
using cura::type::DataType;
using cura::type::TypeId;

TEST(ProjectTest, ColumnRef) {
  Option option;
  auto ctx = makeTrivialContext(option);

  auto column_ref =
      std::make_shared<const ColumnRef>(1, DataType::int32Type(true));
  auto project = std::make_shared<const Project>(
      0, std::vector<std::shared_ptr<const Expression>>{column_ref});
  {
    auto cv_0 = makeDirectColumnVector<std::string>(DataType::stringType(true),
                                                    {"ab", ""}, {true, false});
    auto cv_1 = makeDirectColumnVector<int32_t>(DataType::int32Type(true),
                                                {0, 42}, {false, true});
    auto fragment = makeFragment(std::move(cv_0), std::move(cv_1));

    auto res_fragment = project->stream(ctx, 0, VoidKernelId, fragment, 0);
    ASSERT_NE(res_fragment, nullptr);
    ASSERT_EQ(res_fragment->numColumns(), 1);
    ASSERT_EQ(res_fragment->size(), 2);

    auto res_cvv = res_fragment->column(0);
    auto expected = makeDirectColumnVector<int32_t>(DataType::int32Type(true),
                                                    {0, 42}, {false, true});
    CURA_TEST_EXPECT_COLUMNS_EQUAL(expected, res_cvv);
  }
  {
    auto cv_0 =
        makeDirectColumnVector<std::string>(DataType::stringType(true), {}, {});
    auto fragment = makeFragment(std::move(cv_0));

    ASSERT_THROW(project->stream(ctx, 0, VoidKernelId, fragment, 0),
                 cura::LogicalError);
  }
}
