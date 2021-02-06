#include "table_helper.h"
#include "test_common/rels.h"
#include "utilities/database/evenly_parallel_c_driver.h"
#include "utilities/database/evenly_parallel_cpp_driver.h"
#include "utilities/database/sinks.h"
#include "utilities/execution_helper.h"
#include "utilities/rel_helper.h"

#include <gtest/gtest.h>

using cura::test::database::Database;
using cura::test::database::EvenlyParallelCDriver;
using cura::test::database::EvenlyParallelCppDriver;
using cura::test::database::PrintArrowArrayPerLineSink;
using cura::test::database::Table;
using cura::test::execution::testExecute;
using cura::test::relational::toJson;

TEST(ExecutionTest, OneInputOnly) {
  Table t = makeTableN<int32_t>(DataType::int32Type(), 100, 4, 4);
  auto input_source = makeRelInputSource(100, true);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t});
    testExecute(db, input_source);
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 15>> db({t});
    auto json = toJson(input_source);
    testExecute(db, json);
    testExecute(db, json);
  }
}

TEST(ExecutionTest, SingleLevelUnions) {
  Table t0 = makeTableN<int32_t>(DataType::int32Type(), 100, 2, 10);
  Table t1 = makeTableN<int32_t>(DataType::int32Type(), 200, 3, 5, 3);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1});
    iterateSingleLevelUnionTypes(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1});
    iterateSingleLevelUnionTypes([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    iterateSingleLevelUnionTypes([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, ImbalancedDoubleLevelUnions) {
  Table t0 = makeTableN<int32_t>(DataType::int32Type(), 100, 2, 10);
  Table t1 = makeTableN<int32_t>(DataType::int32Type(), 200, 3, 5, 3);
  Table t2 = makeTableN<int32_t>(DataType::int32Type(), 300, 4, 5, 8);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2});
    iterateImbalancedDoubleLevelUnionTypes(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2});
    iterateImbalancedDoubleLevelUnionTypes([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    iterateImbalancedDoubleLevelUnionTypes([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, BalancedDoubleLevelUnions) {
  Table t0 = makeTableN<int32_t>(DataType::int32Type(), 100, 2, 10);
  Table t1 = makeTableN<int32_t>(DataType::int32Type(), 200, 3, 5, 3);
  Table t2 = makeTableN<int32_t>(DataType::int32Type(), 300, 4, 5, 8);
  Table t3 = makeTableN<int32_t>(DataType::int32Type(), 400, 5, 5, 5);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2, t3});
    iterateBalancedDoubleLevelUnionTypes(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2, t3});
    iterateBalancedDoubleLevelUnionTypes([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    iterateBalancedDoubleLevelUnionTypes([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, HashJoin) {
  Table t0 =
      makeTable<int32_t>(DataType::int32Type(), 100, 2, {22, 32, 42, 52, 62});
  Table t1 = makeTableN<int32_t>(DataType::int32Type(), 200, 3, 10, 40);
  Table t2 = makeTableN<int32_t>(DataType::int32Type(), 300, 4, 20, 40);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2});
    iterateHashJoins(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2});
    iterateHashJoins([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    iterateHashJoins([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, InnerJoin) {
  auto t0 = []() {
    auto c0 = makeArrowColumnVector(DataType::int32Type(),
                                    std::vector<int32_t>{22, 32, 42, 52, 62});
    auto c1 = makeArrowColumnVector(
        DataType::stringType(),
        std::vector<std::string>{"ab", "cd", "ef", "gh", ""});
    auto frag = makeFragment(std::move(c0), std::move(c1));
    Table t{100};
    t.fragments.emplace_back(std::move(frag));
    return t;
  }();
  auto t1 = []() {
    auto c0 = makeArrowColumnVectorN(DataType::int32Type(), 10, 40);
    auto c1 =
        makeArrowColumnVector(DataType::stringType(),
                              std::vector<std::string>{"ab", "cd", "ef", "gh",
                                                       "", "", "", "", "", ""});
    auto frag = makeFragment(std::move(c0), std::move(c1));
    Table t{200};
    t.fragments.emplace_back(std::move(frag));
    return t;
  }();
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1});
    iterateInnerJoins(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1});
    iterateInnerJoins([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    iterateInnerJoins([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, HashJoinWithFilter) {
  Table t0 =
      makeTable<int32_t>(DataType::int32Type(), 100, 2, {22, 32, 42, 52, 62});
  Table t1 = makeTableN<int32_t>(DataType::int32Type(), 200, 3, 10, 40);
  Table t2 = makeTableN<int32_t>(DataType::int32Type(), 300, 4, 20, 40);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2});
    iterateHashJoinWithFilters(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2});
    iterateHashJoinWithFilters([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    iterateHashJoinWithFilters([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, EmptyHashJoinWithFilter) {
  Table t0 = makeTable<int32_t>(DataType::int32Type(), 100, 1, {12});
  Table t1 = makeTableN<int32_t>(DataType::int32Type(), 200, 1, 1, 22);
  Table t2 = makeTableN<int32_t>(DataType::int32Type(), 300, 1, 1, 32);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2});
    iterateHashJoinWithFilters(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 15>> db(
        {t0, t1, t2});
    iterateHashJoinWithFilters([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    iterateHashJoinWithFilters([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, HashJoinTwoUnions) {
  auto makeTable = [](TableId id) {
    auto c0 = makeArrowColumnVector(
        DataType::int64Type(true),
        std::vector<int64_t>{1, 2, 3, 0, 0, 5, 5, 6, 6, 7, 7, 8, 0},
        {true, true, true, false, false, true, true, true, true, true, true,
         true, false});
    auto c1 = makeArrowColumnVector(
        DataType::stringType(true),
        std::vector<std::string>{"ab", "cd", "ef", "gh", "", "ij", "ji", "kl",
                                 "kl", "mn", "mn", "", ""},
        {true, true, true, true, false, true, true, true, true, true, true,
         false, false});
    auto frag = makeFragment(std::move(c0), std::move(c1));
    Table t{id};
    t.fragments.emplace_back(std::move(frag));
    return t;
  };
  Table t1 = makeTable(100);
  Table t2 = makeTable(200);
  Table t3 = makeTable(300);
  Table t4 = makeTable(400);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 16>> db(
        {t1, t2, t3, t4});
    hashJoinWithTwoUnions(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 3>> db(
        {t1, t2, t3, t4});
    hashJoinWithTwoUnions([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    hashJoinWithTwoUnions([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, SimpleProject) {
  auto t = []() {
    auto c0 = makeArrowColumnVectorN(DataType::int32Type(), 5, 42);
    auto c1 = makeArrowColumnVectorN(DataType::int32Type(), 5, 142);
    auto c2 = makeArrowColumnVectorN(DataType::int32Type(), 5, 1142);
    auto c3 = makeArrowColumnVectorN(DataType::int32Type(), 5, 11142);
    auto frag = makeFragment(std::move(c0), std::move(c1), std::move(c2),
                             std::move(c3));
    Table t{100};
    t.fragments.emplace_back(std::move(frag));
    return t;
  }();
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 16>> db(
        {t});
    simpleProject(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 3>> db({t});
    simpleProject([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    simpleProject([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, SimpleAggregate) {
  auto t = []() {
    auto c0 = makeArrowColumnVector(DataType::int32Type(),
                                    std::vector<int32_t>{42, 42, 43, 43, 44});
    auto c1 = makeArrowColumnVectorN(DataType::int32Type(), 5, 142);
    auto c2 = makeArrowColumnVector(
        DataType::stringType(),
        std::vector<std::string>{"42", "42", "43", "44", "45"});
    auto c3 = makeArrowColumnVectorN(DataType::int32Type(), 5, 11142);
    auto frag = makeFragment(std::move(c0), std::move(c1), std::move(c2),
                             std::move(c3));
    Table t{100};
    t.fragments.emplace_back(std::move(frag));
    return t;
  }();
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 16>> db(
        {t});
    simpleAggregate(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 3>> db({t});
    simpleAggregate([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    simpleAggregate([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, AggregateLiteral) {
  auto t = []() {
    auto c = makeArrowColumnVector(DataType::int32Type(),
                                   std::vector<int32_t>{42, 42, 43, 43, 44});
    auto frag = makeFragment(std::move(c));
    Table t{100};
    t.fragments.emplace_back(std::move(frag));
    return t;
  }();
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 16>> db(
        {t});
    aggregateLiteral(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 3>> db({t});
    aggregateLiteral([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    aggregateLiteral([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, ProjectAggregate) {
  auto t = []() {
    auto c0 = makeArrowColumnVectorN(DataType::int32Type(), 5, 42);
    auto c1 = makeArrowColumnVectorN(DataType::int32Type(), 5, 142);
    auto c2 = makeArrowColumnVectorN(DataType::int32Type(), 5, 1142);
    auto c3 = makeArrowColumnVector(
        DataType::stringType(),
        std::vector<std::string>{"42", "42", "43", "44", "45"});
    auto frag = makeFragment(std::move(c0), std::move(c1), std::move(c2),
                             std::move(c3));
    Table t{100};
    t.fragments.emplace_back(std::move(frag));
    return t;
  }();
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 16>> db(
        {t});
    projectAggregate(
        [&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 3>> db({t});
    projectAggregate([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    projectAggregate([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, SimpleSort) {
  auto t = []() {
    auto c0 =
        makeArrowColumnVector(DataType::int32Type(true),
                              std::vector<std::int32_t>{42, 0, 42, 1, 0, 0},
                              {true, true, true, false, false, true});
    auto c1 = makeArrowColumnVectorN(DataType::int32Type(), 6, 42);
    auto c2 = makeArrowColumnVector(
        DataType::stringType(true),
        std::vector<std::string>{"y", "a", "z", "i", "j", "b"},
        {false, true, true, true, false, true});
    auto frag = makeFragment(std::move(c0), std::move(c1), std::move(c2));
    Table t{100};
    t.fragments.emplace_back(std::move(frag));
    return t;
  }();
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 16>> db(
        {t});
    simpleSort([&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); });
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 3>> db({t});
    simpleSort([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
    simpleSort([&](std::shared_ptr<const Rel> rel) {
      auto json = toJson(rel);
      testExecute(db, json);
    });
  }
}

TEST(ExecutionTest, SimpleLimit) {
  Table t = makeTableN<int32_t>(DataType::int32Type(), 100, 2, 4, 40);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 16>> db(
        {t});
    simpleLimit([&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); },
                1, 7);
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 3>> db({t});
    simpleLimit(
        [&](std::shared_ptr<const Rel> rel) {
          auto json = toJson(rel);
          testExecute(db, json);
        },
        1, 7);
    simpleLimit(
        [&](std::shared_ptr<const Rel> rel) {
          auto json = toJson(rel);
          testExecute(db, json);
        },
        1, 7);
  }
}

TEST(ExecutionTest, TopN) {
  Table t = makeTableN<int32_t>(DataType::int32Type(), 100, 2, 4, 40);
  {
    Database<EvenlyParallelCppDriver<PrintArrowArrayPerLineSink, 8, 16>> db(
        {t});
    topN([&](std::shared_ptr<const Rel> rel) { testExecute(db, rel); }, 7);
  }
  {
    Database<EvenlyParallelCDriver<PrintArrowArrayPerLineSink, 8, 3>> db({t});
    topN(
        [&](std::shared_ptr<const Rel> rel) {
          auto json = toJson(rel);
          testExecute(db, json);
        },
        7);
    topN(
        [&](std::shared_ptr<const Rel> rel) {
          auto json = toJson(rel);
          testExecute(db, json);
        },
        7);
  }
}
