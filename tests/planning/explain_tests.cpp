#include "cura/driver/option.h"
#include "cura/planning/planner.h"
#include "test_common/rels.h"

#include <gtest/gtest.h>

using cura::driver::Option;
using cura::planning::Planner;

void testPlanner(std::shared_ptr<const Rel> rel) {
  Option option;
  Planner planner(option);
  auto result = planner.explain(rel, true);
  for (const auto &line : result) {
    std::cout << line << std::endl;
  }
}

TEST(ExplainTest, OneInputOnly) {
  auto input_source = makeRelInputSource(100, true);
  testPlanner(input_source);
}

TEST(ExplainTest, SingleLevelUnions) {
  iterateSingleLevelUnionTypes(testPlanner);
}

TEST(ExplainTest, ImbalancedDoubleLevelUnions) {
  iterateImbalancedDoubleLevelUnionTypes(testPlanner);
}

TEST(ExplainTest, BalancedDoubleLevelUnions) {
  iterateBalancedDoubleLevelUnionTypes(testPlanner);
}

TEST(ExplainTest, HashJoin) { iterateHashJoins(testPlanner); }

TEST(ExplainTest, InnerJoin) { iterateInnerJoins(testPlanner); }

TEST(ExplainTest, HashJoinWithFilter) {
  iterateHashJoinWithFilters(testPlanner);
}

TEST(ExplainTest, HashJoinWithTwoUnions) { hashJoinWithTwoUnions(testPlanner); }

TEST(ExplainTest, SimpleProject) { simpleProject(testPlanner); }

TEST(ExplainTest, SimpleAggregate) { simpleAggregate(testPlanner); }

TEST(ExplainTest, AggregateLiteral) { aggregateLiteral(testPlanner); }

TEST(ExplainTest, ProjectAggregate) { projectAggregate(testPlanner); }

TEST(ExplainTest, SimpleSort) { simpleSort(testPlanner); }
