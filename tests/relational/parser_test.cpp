#include "cura/driver/option.h"
#include "cura/planning/planner.h"
#include "cura/relational/parsers.h"
#include "test_common/rels.h"
#include "utilities/rel_helper.h"

#include <gtest/gtest.h>
#include <rapidjson/document.h>

using cura::driver::Option;
using cura::planning::Planner;
using cura::relational::parseJson;
using cura::relational::RelProject;
using cura::test::relational::toJson;

void explainRel(std::shared_ptr<const Rel> rel, bool extended) {
  Option option;
  Planner planner(option);
  auto result = planner.explain(rel, extended);
  for (const auto &line : result) {
    std::cout << line << std::endl;
  }
}

void assertJsonEqual(const std::string &json1, const std::string &json2) {
  rapidjson::Document doc1, doc2;
  doc1.Parse(json1.data());
  doc2.Parse(json2.data());
  ASSERT_FALSE(doc1.HasParseError());
  ASSERT_FALSE(doc2.HasParseError());
  if (doc1 != doc2) {
    std::cout << "Json1: " << json1 << std::endl;
    std::cout << "Json2: " << json2 << std::endl;
  }
  ASSERT_EQ(doc1, doc2);
}

void testParser(std::shared_ptr<const Rel> rel) {
  std::cout << "======= Original Plan =======" << std::endl;
  explainRel(rel, true);
  auto json = toJson(rel);
  std::cout << "======= Intermediate Json =======" << std::endl;
  std::cout << json << std::endl;
  std::cout << "======= Re-parsed Plan =======" << std::endl;
  auto parsed = parseJson(json);
  explainRel(parsed, true);
  auto parsed_json = toJson(parsed);
  assertJsonEqual(json, parsed_json);
}

TEST(ParserTest, OneInputOnly) {
  auto input_source = makeRelInputSource(100);
  testParser(input_source);
}

TEST(ParserTest, SingleLevelUnions) {
  iterateSingleLevelUnionTypes(testParser);
}

TEST(ParserTest, ImbalancedDoubleLevelUnions) {
  iterateImbalancedDoubleLevelUnionTypes(testParser);
}

TEST(ParserTest, BalancedDoubleLevelUnions) {
  iterateBalancedDoubleLevelUnionTypes(testParser);
}

TEST(ParserTest, HashJoin) { iterateHashJoins(testParser); }

TEST(ParserTest, HashJoinWithFilter) { iterateHashJoinWithFilters(testParser); }

template <typename T>
void testExpression(const std::string &json,
                    std::function<void(std::shared_ptr<const T>)> f) {
  auto prefix =
      R"({"rels": [
{
"rel_op": "InputSource",
"source_id": 100,
"schema": [{"type": "INT32", "nullable": true}, {"type": "STRING", "nullable": false}]
},
{
"rel_op": "Project",
"exprs": [)";
  auto suffix = R"(]}
]})";
  auto full_json = prefix + json + suffix;
  auto rel = parseJson(full_json);
  explainRel(rel, false);
  auto project = std::dynamic_pointer_cast<const RelProject>(rel);
  ASSERT_NE(project, nullptr);
  auto expression =
      std::dynamic_pointer_cast<const T>(project->expressions()[0]);
  ASSERT_NE(expression, nullptr);
  f(expression);
  auto parsed_json = toJson(rel);
  assertJsonEqual(full_json, parsed_json);
}

TEST(ParserTest, Literal) {
  {
    auto json = R"({"type": "INT32", "literal": null})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::int32Type(true));
    });
  }
  {
    auto json = R"({"type": "BOOL8", "literal": true})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::bool8Type());
      ASSERT_EQ(literal->value<bool>(), true);
    });
  }
  {
    auto json = R"({"type": "INT32", "literal": -42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::int32Type());
      ASSERT_EQ(literal->value<int32_t>(), -42);
    });
  }
  {
    auto json = R"({"type": "UINT64", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::uint64Type());
      ASSERT_EQ(literal->value<uint64_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "FLOAT64", "literal": 42.42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::float64Type());
      ASSERT_DOUBLE_EQ(literal->value<double>(), 42.42);
    });
  }
  {
    auto json = R"({"type": "STRING", "literal": "abc"})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::stringType());
      ASSERT_EQ(literal->value<std::string>(), "abc");
    });
  }
  {
    auto json = R"({"type": "TIMESTAMP_DAYS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::timestampDaysType());
      ASSERT_EQ(literal->value<int32_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "TIMESTAMP_SECONDS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::timestampSecondsType());
      ASSERT_EQ(literal->value<int64_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "TIMESTAMP_MILLISECONDS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::timestampMillisecondsType());
      ASSERT_EQ(literal->value<int64_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "TIMESTAMP_MICROSECONDS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::timestampMicorsecondsType());
      ASSERT_EQ(literal->value<int64_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "TIMESTAMP_NANOSECONDS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::timestampNanosecondsType());
      ASSERT_EQ(literal->value<int64_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "DURATION_SECONDS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::durationSecondsType());
      ASSERT_EQ(literal->value<int64_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "DURATION_MILLISECONDS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::durationMillisecondsType());
      ASSERT_EQ(literal->value<int64_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "DURATION_MICROSECONDS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::durationMicorsecondsType());
      ASSERT_EQ(literal->value<int64_t>(), 42);
    });
  }
  {
    auto json = R"({"type": "DURATION_NANOSECONDS", "literal": 42})";
    testExpression<Literal>(json, [](std::shared_ptr<const Literal> literal) {
      ASSERT_EQ(literal->dataType(), DataType::durationNanosecondsType());
      ASSERT_EQ(literal->value<int64_t>(), 42);
    });
  }
}

TEST(ParserTest, ColumnRef) {
  {
    auto json = R"({"col_ref": 0})";
    testExpression<ColumnRef>(
        json, [](std::shared_ptr<const ColumnRef> col_ref) {
          ASSERT_EQ(col_ref->dataType(), DataType::int32Type(true));
          ASSERT_EQ(col_ref->columnIdx(), 0);
        });
  }
  {
    auto json = R"({"col_ref": 1})";
    testExpression<ColumnRef>(
        json, [](std::shared_ptr<const ColumnRef> col_ref) {
          ASSERT_EQ(col_ref->dataType(), DataType::stringType());
          ASSERT_EQ(col_ref->columnIdx(), 1);
        });
  }
}

TEST(ParserTest, UnaryOp) {
  {
    auto json =
        R"({"unary_op": "NOT", "operands": [{"type": "BOOL8", "literal": true}], "type": {"type": "BOOL8", "nullable": false}})";
    testExpression<UnaryOp>(json, [](std::shared_ptr<const UnaryOp> unary_op) {
      ASSERT_EQ(unary_op->dataType(), DataType::bool8Type());
      ASSERT_EQ(unary_op->unaryOperator(), UnaryOp::UnaryOperator::NOT);
    });
  }
}

TEST(ParserTest, TiUnaryOp) {
  {
    auto json =
        R"({"ti_unary_op": "EXTRACT_YEAR", "operands": [{"type": "INT64", "literal": 42}], "type": {"type": "INT64", "nullable": false}})";
    testExpression<TiUnaryOp>(
        json, [](std::shared_ptr<const TiUnaryOp> ti_unary_op) {
          ASSERT_EQ(ti_unary_op->dataType(), DataType::int64Type());
          ASSERT_EQ(ti_unary_op->unaryOperator(),
                    TiUnaryOp::UnaryOperator::EXTRACT_YEAR);
        });
  }
}

TEST(ParserTest, BinaryOp) {
  {
    auto json =
        R"({"binary_op": "ADD", "operands": [{"type": "INT64", "literal": 42}, {"col_ref": 0}], "type": {"type": "INT64", "nullable": true}})";
    testExpression<BinaryOp>(
        json, [](std::shared_ptr<const BinaryOp> binary_op) {
          ASSERT_EQ(binary_op->dataType(), DataType::int64Type(true));
          ASSERT_EQ(binary_op->binaryOperator(), BinaryOperator::ADD);
        });
  }
  {
    auto json =
        R"({"binary_op": "PMOD", "operands": [{"type": "INT64", "literal": 42}, {"col_ref": 0}], "type": {"type": "INT64", "nullable": true}})";
    testExpression<BinaryOp>(
        json, [](std::shared_ptr<const BinaryOp> binary_op) {
          ASSERT_EQ(binary_op->dataType(), DataType::int64Type(true));
          ASSERT_EQ(binary_op->binaryOperator(), BinaryOperator::PMOD);
        });
  }
}
