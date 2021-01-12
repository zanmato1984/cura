#include "cura/relational/parsers.h"
#include "cura/common/errors.h"
#include "cura/expression/aggregation.h"
#include "cura/expression/binary_op.h"
#include "cura/expression/literal.h"
#include "cura/expression/ti_unary_op.h"
#include "cura/expression/unary_op.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <stack>

namespace cura::relational {

using cura::expression::Aggregation;
using cura::expression::AggregationOperator;
using cura::expression::aggregationOperatorFromString;
using cura::expression::BinaryOp;
using cura::expression::binaryOperatorFromString;
using cura::expression::ColumnIdx;
using cura::expression::Literal;
using cura::expression::NthElement;
using cura::expression::TiUnaryOp;
using cura::expression::UnaryOp;
using cura::relational::BuildSide;
using cura::type::TypeId;
using cura::type::typeIdFromString;

namespace detail {

inline const rapidjson::Value &jsonField(const rapidjson::Value &obj,
                                         const std::string &field) {
  CURA_ASSERT_JSON(obj.IsObject());
  const auto field_it = obj.FindMember(field.data());
  CURA_ASSERT(field_it != obj.MemberEnd(), "Couldn't find field " + field);
  return field_it->value;
}

inline bool jsonBool(const rapidjson::Value &obj) {
  CURA_ASSERT_JSON(obj.IsBool());
  return obj.GetBool();
}

inline int64_t jsonInt64(const rapidjson::Value &obj) {
  CURA_ASSERT_JSON(obj.IsInt64());
  return obj.GetInt64();
}

inline uint64_t jsonUint64(const rapidjson::Value &obj) {
  CURA_ASSERT_JSON(obj.IsUint64());
  return obj.GetUint64();
}

inline double jsonFloat(const rapidjson::Value &obj) {
  CURA_ASSERT_JSON(obj.IsFloat() || obj.IsInt() || obj.IsUint());
  return obj.GetFloat();
}

inline double jsonDouble(const rapidjson::Value &obj) {
  CURA_ASSERT_JSON(obj.IsDouble() || obj.IsFloat() || obj.IsInt() ||
                   obj.IsInt64() || obj.IsUint() || obj.IsUint64());
  return obj.GetDouble();
}

inline std::string jsonString(const rapidjson::Value &obj) {
  CURA_ASSERT_JSON(obj.IsString());
  return obj.GetString();
}

DataType parseDataType(const rapidjson::Value &json) {
  CURA_ASSERT_JSON(json.IsObject());
  const auto &type_json = jsonField(json, "type");
  auto type_id = typeIdFromString(jsonString(type_json));
  const auto &nullable_json = jsonField(json, "nullable");
  auto nullable = jsonBool(nullable_json);
  return DataType(type_id, nullable);
}

std::shared_ptr<const Expression> parseLiteral(const rapidjson::Value &json) {
  const auto &type_json = jsonField(json, "type");
  auto type_id = typeIdFromString(jsonString(type_json));
  const auto &literal_json = jsonField(json, "literal");
  if (literal_json.IsNull()) {
    return std::make_shared<Literal>(type_id);
  }
  switch (type_id) {
  case TypeId::BOOL8: {
    auto literal = jsonBool(literal_json);
    return std::make_shared<Literal>(type_id, literal);
  }
  case TypeId::INT8: {
    auto literal = jsonInt64(literal_json);
    return std::make_shared<Literal>(type_id, static_cast<int8_t>(literal));
  }
  case TypeId::INT16: {
    auto literal = jsonInt64(literal_json);
    return std::make_shared<Literal>(type_id, static_cast<int16_t>(literal));
  }
  case TypeId::INT32: {
    auto literal = jsonInt64(literal_json);
    return std::make_shared<Literal>(type_id, static_cast<int32_t>(literal));
  }
  case TypeId::INT64: {
    auto literal = jsonInt64(literal_json);
    return std::make_shared<Literal>(type_id, literal);
  }
  case TypeId::UINT8: {
    auto literal = jsonUint64(literal_json);
    return std::make_shared<Literal>(type_id, static_cast<uint8_t>(literal));
  }
  case TypeId::UINT16: {
    auto literal = jsonUint64(literal_json);
    return std::make_shared<Literal>(type_id, static_cast<uint32_t>(literal));
  }
  case TypeId::UINT32: {
    auto literal = jsonUint64(literal_json);
    return std::make_shared<Literal>(type_id, static_cast<uint64_t>(literal));
  }
  case TypeId::UINT64: {
    auto literal = jsonUint64(literal_json);
    return std::make_shared<Literal>(type_id, literal);
  }
  case TypeId::FLOAT32: {
    auto literal = jsonFloat(literal_json);
    return std::make_shared<Literal>(type_id, literal);
  }
  case TypeId::FLOAT64: {
    auto literal = jsonDouble(literal_json);
    return std::make_shared<Literal>(type_id, literal);
  }
  case TypeId::STRING: {
    auto literal = jsonString(literal_json);
    return std::make_shared<Literal>(type_id, literal);
  }
  case TypeId::TIMESTAMP_DAYS:
  case TypeId::TIMESTAMP_SECONDS:
  case TypeId::TIMESTAMP_MILLISECONDS:
  case TypeId::TIMESTAMP_MICROSECONDS:
  case TypeId::TIMESTAMP_NANOSECONDS:
  case TypeId::DURATION_SECONDS:
  case TypeId::DURATION_MILLISECONDS:
  case TypeId::DURATION_MICROSECONDS:
  case TypeId::DURATION_NANOSECONDS: {
    auto literal = jsonInt64(literal_json);
    return std::make_shared<Literal>(type_id, literal);
  }
  default:
    CURA_FAIL("Invalid literal type " +
              std::to_string(static_cast<int32_t>(type_id)));
  }
}

std::shared_ptr<const Expression> parseColumnRef(const rapidjson::Value &json,
                                                 const Schema &schema) {
  const auto &col_ref_json = jsonField(json, "col_ref");
  auto col_id = static_cast<ColumnIdx>(jsonInt64(col_ref_json));
  CURA_ASSERT(col_id < schema.size(), "Invalid column index");
  const auto &data_type = schema[col_id];
  return std::make_shared<ColumnRef>(col_id, data_type);
}

std::vector<std::shared_ptr<const Expression>>
parseExpressions(const rapidjson::Value &json, const Schema &schema);

template <typename T>
std::shared_ptr<const Expression> parseUnaryOp(const rapidjson::Value &json,
                                               const std::string &unary_op_key,
                                               const Schema &schema) {
  const auto &operands_json = jsonField(json, "operands");
  auto operands = parseExpressions(operands_json, schema);
  CURA_ASSERT(operands.size() == 1, "Unary op operand size must be 1");
  const auto &dt_json = jsonField(json, "type");
  auto data_type = parseDataType(dt_json);
  const auto &op_json = jsonField(json, unary_op_key);
  auto unary_op = T::unaryOperatorFromString(jsonString(op_json));
  return std::make_shared<T>(operands[0], std::move(data_type), unary_op);
}

std::shared_ptr<const Expression> parseBinaryOp(const rapidjson::Value &json,
                                                const Schema &schema) {
  const auto &op_json = jsonField(json, "binary_op");
  auto binary_op = binaryOperatorFromString(jsonString(op_json));
  const auto &operands_json = jsonField(json, "operands");
  auto operands = parseExpressions(operands_json, schema);
  CURA_ASSERT(operands.size() == 2, "Binary op operand size must be 2");
  const auto &dt_json = jsonField(json, "type");
  auto data_type = parseDataType(dt_json);
  return std::make_shared<BinaryOp>(binary_op, operands[0], operands[1],
                                    std::move(data_type));
}

std::shared_ptr<const Expression> parseAggregation(const rapidjson::Value &json,
                                                   const Schema &schema) {
  const auto &op_json = jsonField(json, "agg");
  auto op = aggregationOperatorFromString(jsonString(op_json));
  const auto &operands_json = jsonField(json, "operands");
  auto operands = parseExpressions(operands_json, schema);
  CURA_ASSERT(operands.size() == 1, "Aggregation operand size must be 1");
  const auto &dt_json = jsonField(json, "type");
  auto data_type = parseDataType(dt_json);
  // Aggregation-specific parsing.
  if (op == AggregationOperator::NTH_ELEMENT) {
    const auto &n_json = jsonField(json, "n");
    auto n = jsonInt64(n_json);
    return std::make_shared<NthElement>(operands[0], std::move(data_type), n);
  } else {
    return std::make_shared<Aggregation>(op, operands[0], std::move(data_type));
  }
}

std::shared_ptr<const Expression> parseExpression(const rapidjson::Value &json,
                                                  const Schema &schema) {
  CURA_ASSERT_JSON(json.IsObject());
  if (json.HasMember("literal")) {
    return parseLiteral(json);
  }
  if (json.HasMember("col_ref")) {
    return parseColumnRef(json, schema);
  }
  if (json.HasMember("unary_op")) {
    return parseUnaryOp<UnaryOp>(json, "unary_op", schema);
  }
  if (json.HasMember("ti_unary_op")) {
    return parseUnaryOp<TiUnaryOp>(json, "ti_unary_op", schema);
  }
  if (json.HasMember("binary_op")) {
    return parseBinaryOp(json, schema);
  }
  if (json.HasMember("agg")) {
    return parseAggregation(json, schema);
  }
  CURA_FAIL("Unknown expression");
}

std::vector<std::shared_ptr<const Expression>>
parseExpressions(const rapidjson::Value &json, const Schema &schema) {
  CURA_ASSERT_JSON(json.IsArray());
  std::vector<std::shared_ptr<const Expression>> expressions;
  for (auto it = json.Begin(); it != json.End(); it++) {
    const auto &expression_json = *it;
    expressions.emplace_back(parseExpression(expression_json, schema));
  }
  return expressions;
}

Schema parseSchema(const rapidjson::Value &json) {
  CURA_ASSERT_JSON(json.IsArray());
  Schema schema;
  for (auto it = json.Begin(); it != json.End(); it++) {
    const auto &dt_json = *it;
    schema.emplace_back(parseDataType(dt_json));
  }
  return schema;
}

void parseInputSource(const rapidjson::Value &rel,
                      std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  const auto &source_id_json = jsonField(rel, "source_id");
  SourceId source_id = jsonInt64(source_id_json);
  const auto &schema_json = jsonField(rel, "schema");
  auto schema = parseSchema(schema_json);
  auto input_source =
      std::make_shared<RelInputSource>(source_id, std::move(schema));
  rel_stack.emplace(input_source);
}

void parseFilter(const rapidjson::Value &rel,
                 std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  CURA_ASSERT(!rel_stack.empty(), "No child of RelFilter");
  auto child = rel_stack.top();
  rel_stack.pop();
  const auto &cond_json = jsonField(rel, "condition");
  auto cond = parseExpression(cond_json, child->output());
  auto filter = std::make_shared<RelFilter>(child, cond);
  rel_stack.emplace(filter);
}

template <typename UnionType>
void parseUnion(const rapidjson::Value &rel,
                std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  CURA_ASSERT(rel_stack.size() >= 2, "No child of RelFilter");
  auto child_1 = rel_stack.top();
  rel_stack.pop();
  auto child_0 = rel_stack.top();
  rel_stack.pop();
  auto u = std::make_shared<UnionType>(
      std::vector<std::shared_ptr<const Rel>>{child_0, child_1});
  rel_stack.emplace(u);
}

void parseHashJoin(const rapidjson::Value &rel,
                   std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  const auto &type_json = jsonField(rel, "type");
  auto type_string = jsonString(type_json);
  auto join_type = joinTypeFromString(type_string);
  CURA_ASSERT(rel_stack.size() >= 2, "No child of RelFilter");
  auto right = rel_stack.top();
  rel_stack.pop();
  auto left = rel_stack.top();
  rel_stack.pop();
  Schema schema = left->output();
  schema.insert(schema.end(), right->output().begin(), right->output().end());
  auto build_side = BuildSide::RIGHT;
  if (rel.HasMember("build_side")) {
    CURA_ASSERT(join_type == JoinType::INNER,
                "Specifying build side can only be used for inner join");
    const auto &build_side_json = jsonField(rel, "build_side");
    auto build_side_str = jsonString(build_side_json);
    build_side = buildSideFromString(build_side_str);
  }
  const auto &cond_json = jsonField(rel, "condition");
  auto cond = parseExpression(cond_json, schema);
  auto hash_join =
      std::make_shared<RelHashJoin>(join_type, left, right, build_side, cond);
  rel_stack.emplace(hash_join);
}

void parseProject(const rapidjson::Value &rel,
                  std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  CURA_ASSERT(!rel_stack.empty(), "No child of RelProject");
  auto child = rel_stack.top();
  rel_stack.pop();
  const auto &exprs_json = jsonField(rel, "exprs");
  auto exprs = parseExpressions(exprs_json, child->output());
  auto project = std::make_shared<RelProject>(child, std::move(exprs));
  rel_stack.emplace(project);
}

void parseAggregate(const rapidjson::Value &rel,
                    std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  CURA_ASSERT(!rel_stack.empty(), "No child of RelAggregate");
  auto child = rel_stack.top();
  rel_stack.pop();
  const auto &groups_json = jsonField(rel, "groups");
  auto groups = parseExpressions(groups_json, child->output());
  const auto &aggs_json = jsonField(rel, "aggs");
  auto aggs = parseExpressions(aggs_json, child->output());
  auto aggregate =
      std::make_shared<RelAggregate>(child, std::move(groups), std::move(aggs));
  rel_stack.emplace(aggregate);
}

void parseSort(const rapidjson::Value &rel,
               std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  CURA_ASSERT(!rel_stack.empty(), "No child of RelSort");
  auto child = rel_stack.top();
  rel_stack.pop();
  const auto &sort_infos_json = jsonField(rel, "sort_infos");
  CURA_ASSERT_JSON(sort_infos_json.IsArray());
  std::vector<SortInfo> sort_infos;
  for (auto it = sort_infos_json.Begin(); it != sort_infos_json.End(); it++) {
    const auto &sort_info_json = *it;
    const auto &expr_json = jsonField(sort_info_json, "expr");
    auto expr = parseExpression(expr_json, child->output());
    auto order = SortInfo::Order::ASCENDING;
    if (sort_info_json.HasMember("order")) {
      const auto order_str = jsonString(jsonField(sort_info_json, "order"));
      if (order_str == "ASC") {
      } else if (order_str == "DESC") {
        order = SortInfo::Order::DESCENDING;
      } else {
        CURA_FAIL("Unknown order " + order_str);
      }
    }
    auto null_order = SortInfo::NullOrder::FIRST;
    if (sort_info_json.HasMember("null_order")) {
      const auto null_order_str =
          jsonString(jsonField(sort_info_json, "null_order"));
      if (null_order_str == "NULLS_FIRST") {
      } else if (null_order_str == "NULLS_LAST") {
        null_order = SortInfo::NullOrder::LAST;
      } else {
        CURA_FAIL("Unknown null order " + null_order_str);
      }
    }
    sort_infos.emplace_back(SortInfo{expr, order, null_order});
  }
  auto sort = std::make_shared<RelSort>(child, std::move(sort_infos));
  rel_stack.emplace(sort);
}

void parseLimit(const rapidjson::Value &rel,
                std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  CURA_ASSERT(!rel_stack.empty(), "No child of RelLimit");
  auto child = rel_stack.top();
  rel_stack.pop();
  const auto &n_json = jsonField(rel, "n");
  CURA_ASSERT_JSON(n_json.IsUint64() || n_json.IsUint());
  size_t n = jsonUint64(n_json);
  size_t offset = 0;
  if (rel.HasMember("offset")) {
    const auto &offset_json = jsonField(rel, "offset");
    CURA_ASSERT_JSON(offset_json.IsUint64() || offset_json.IsUint());
    offset = jsonUint64(offset_json);
  }
  auto limit = std::make_shared<RelLimit>(child, offset, n);
  rel_stack.emplace(limit);
}

void dispatchRel(const rapidjson::Value &rel,
                 std::stack<std::shared_ptr<const Rel>> &rel_stack) {
  const auto rel_op = jsonString(jsonField(rel, "rel_op"));
  if (rel_op == "InputSource") {
    return parseInputSource(rel, rel_stack);
  } else if (rel_op == "Filter") {
    return parseFilter(rel, rel_stack);
  } else if (rel_op == "Union") {
    return parseUnion<RelUnion>(rel, rel_stack);
  } else if (rel_op == "UnionAll") {
    return parseUnion<RelUnionAll>(rel, rel_stack);
  } else if (rel_op == "HashJoin") {
    return parseHashJoin(rel, rel_stack);
  } else if (rel_op == "Project") {
    return parseProject(rel, rel_stack);
  } else if (rel_op == "Aggregate") {
    return parseAggregate(rel, rel_stack);
  } else if (rel_op == "Sort") {
    return parseSort(rel, rel_stack);
  } else if (rel_op == "Limit") {
    return parseLimit(rel, rel_stack);
  } else {
    CURA_FAIL("Unknown Rel type " + rel_op);
  }
}

} // namespace detail

std::shared_ptr<const Rel> parseJson(const std::string &json) {
  rapidjson::Document doc;
  doc.Parse(json.data());
  if (doc.HasParseError()) {
    doc.GetParseError();
    CURA_FAIL("Failed to parse plan from json (offset " +
              std::to_string(doc.GetErrorOffset()) +
              "): " + rapidjson::GetParseError_En(doc.GetParseError()));
  }
  CURA_ASSERT_JSON(doc.IsObject());
  const auto &rels = detail::jsonField(doc, "rels");
  CURA_ASSERT_JSON(rels.IsArray());
  std::stack<std::shared_ptr<const Rel>> rel_stack;
  for (auto it = rels.Begin(); it != rels.End(); it++) {
    const auto &rel = *it;
    detail::dispatchRel(rel, rel_stack);
  }
  CURA_ASSERT(rel_stack.size() == 1, "Invalid json plan");
  return rel_stack.top();
}

} // namespace cura::relational
