#include "rel_helper.h"
#include "cura/expression/aggregation.h"
#include "cura/expression/binary_op.h"
#include "cura/expression/expressions.h"
#include "cura/expression/literal.h"
#include "cura/expression/ti_unary_op.h"
#include "cura/expression/unary_op.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

namespace cura::test::relational {

namespace detail {

using namespace cura::relational;
using namespace cura::expression;
using namespace cura::type;

void flattenRel(std::shared_ptr<const Rel> rel,
                std::vector<std::shared_ptr<const Rel>> &rels) {
  for (const auto &child : rel->inputs) {
    flattenRel(child, rels);
  }
  rels.emplace_back(rel);
}

void dataTypeToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                    const DataType &data_type) {
  writer.StartObject();
  writer.Key("type");
  writer.String(typeIdToString(data_type.type_id).data());
  writer.Key("nullable");
  writer.Bool(data_type.nullable);
  writer.EndObject();
}

void literalToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                   std::shared_ptr<const Literal> literal) {
  writer.Key("type");
  writer.String(typeIdToString(literal->dataType().type_id).data());
  writer.Key("literal");
  if (literal->dataType().nullable) {
    writer.Null();
    return;
  }
  switch (literal->dataType().type_id) {
  case TypeId::BOOL8:
    writer.Bool(literal->value<bool>());
    break;
  case TypeId::INT8:
    writer.Int64(literal->value<int8_t>());
    break;
  case TypeId::INT16:
    writer.Int64(literal->value<int16_t>());
    break;
  case TypeId::INT32:
    writer.Int64(literal->value<int32_t>());
    break;
  case TypeId::INT64:
    writer.Int64(literal->value<int64_t>());
    break;
  case TypeId::UINT8:
    writer.Uint64(literal->value<uint8_t>());
    break;
  case TypeId::UINT16:
    writer.Uint64(literal->value<uint16_t>());
    break;
  case TypeId::UINT32:
    writer.Uint64(literal->value<uint32_t>());
    break;
  case TypeId::UINT64:
    writer.Uint64(literal->value<uint64_t>());
    break;
  case TypeId::FLOAT32:
    writer.Double(literal->value<float>());
    break;
  case TypeId::FLOAT64:
    writer.Double(literal->value<double>());
    break;
  case TypeId::STRING:
    writer.String(literal->value<std::string>().data());
    break;
  case TypeId::TIMESTAMP_DAYS:
    writer.Int64(literal->value<int32_t>());
    break;
  case TypeId::TIMESTAMP_SECONDS:
  case TypeId::TIMESTAMP_MILLISECONDS:
  case TypeId::TIMESTAMP_MICROSECONDS:
  case TypeId::TIMESTAMP_NANOSECONDS:
  case TypeId::DURATION_SECONDS:
  case TypeId::DURATION_MILLISECONDS:
  case TypeId::DURATION_MICROSECONDS:
  case TypeId::DURATION_NANOSECONDS:
    writer.Int64(literal->value<int64_t>());
    break;
  default:
    CURA_FAIL("Invalid literal value " + literal->toString());
  }
}

void columnRefToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                     std::shared_ptr<const ColumnRef> column_ref) {
  writer.Key("col_ref");
  writer.Int64(column_ref->columnIdx());
}

void expressionsToJson(
    rapidjson::Writer<rapidjson::StringBuffer> &writer,
    const std::vector<std::shared_ptr<const Expression>> &expressions);

template <typename T>
void unaryOpToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                   std::shared_ptr<const T> unary_op,
                   const std::string &unary_op_key) {
  writer.Key(unary_op_key.data());
  writer.String(T::unaryOperatorToString(unary_op->unaryOperator()).data());
  writer.Key("operands");
  expressionsToJson(writer, unary_op->operands());
  writer.Key("type");
  dataTypeToJson(writer, unary_op->dataType());
}

void binaryOpToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                    std::shared_ptr<const BinaryOp> binary_op) {
  writer.Key("binary_op");
  writer.String(binaryOperatorToString(binary_op->binaryOperator()).data());
  writer.Key("operands");
  expressionsToJson(writer, binary_op->operands());
  writer.Key("type");
  dataTypeToJson(writer, binary_op->dataType());
}

void aggregationToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                       std::shared_ptr<const Aggregation> aggregation) {
  writer.Key("agg");
  writer.String(
      aggregationOperatorToString(aggregation->aggregationOperator()).data());
  writer.Key("operands");
  expressionsToJson(writer, aggregation->operands());
  writer.Key("type");
  dataTypeToJson(writer, aggregation->dataType());
}

void expressionToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                      std::shared_ptr<const Expression> expression) {
  writer.StartObject();
  if (auto literal = std::dynamic_pointer_cast<const Literal>(expression);
      literal) {
    literalToJson(writer, literal);
  } else if (auto column_ref =
                 std::dynamic_pointer_cast<const ColumnRef>(expression);
             column_ref) {
    columnRefToJson(writer, column_ref);
  } else if (auto unary_op =
                 std::dynamic_pointer_cast<const UnaryOp>(expression);
             unary_op) {
    unaryOpToJson(writer, unary_op, "unary_op");
  } else if (auto ti_unary_op =
                 std::dynamic_pointer_cast<const TiUnaryOp>(expression);
             ti_unary_op) {
    unaryOpToJson(writer, ti_unary_op, "ti_unary_op");
  } else if (auto binary_op =
                 std::dynamic_pointer_cast<const BinaryOp>(expression);
             binary_op) {
    binaryOpToJson(writer, binary_op);
  } else if (auto aggregation =
                 std::dynamic_pointer_cast<const Aggregation>(expression);
             aggregation) {
    aggregationToJson(writer, aggregation);
  }
  writer.EndObject();
}

void expressionsToJson(
    rapidjson::Writer<rapidjson::StringBuffer> &writer,
    const std::vector<std::shared_ptr<const Expression>> &expressions) {
  writer.StartArray();
  for (const auto &expression : expressions) {
    expressionToJson(writer, expression);
  }
  writer.EndArray();
}

void schemaToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                  const Schema &schema) {
  writer.StartArray();
  for (const auto &dt : schema) {
    dataTypeToJson(writer, dt);
  }
  writer.EndArray();
}

void inputSourceToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                       std::shared_ptr<const RelInputSource> input_source) {
  writer.Key("source_id");
  writer.Int64(input_source->sourceId());
  writer.Key("schema");
  schemaToJson(writer, input_source->schema());
}

void filterToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                  std::shared_ptr<const RelFilter> filter) {
  writer.Key("condition");
  expressionToJson(writer, filter->condition());
}

void unionToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                 std::shared_ptr<const RelUnion> u) {}

void unionAllToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                    std::shared_ptr<const RelUnionAll> ua) {}

void hashJoinToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                    std::shared_ptr<const RelHashJoin> hj) {
  writer.Key("type");
  writer.String(joinTypeToString(hj->joinType()).data());
  if (hj->joinType() == JoinType::INNER) {
    writer.Key("build_side");
    writer.String(buildSideToString(hj->buildSide()).data());
  }
  writer.Key("condition");
  expressionToJson(writer, hj->condition());
}

void projectToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                   std::shared_ptr<const RelProject> project) {
  writer.Key("exprs");
  expressionsToJson(writer, project->expressions());
}

void aggregateToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                     std::shared_ptr<const RelAggregate> aggregate) {
  writer.Key("groups");
  expressionsToJson(writer, aggregate->groups());
  writer.Key("aggs");
  expressionsToJson(writer, aggregate->aggregations());
}

void sortToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                std::shared_ptr<const RelSort> sort) {
  writer.Key("sort_infos");
  writer.StartArray();
  for (const auto &sort_info : sort->sortInfos()) {
    writer.StartObject();
    writer.Key("expr");
    expressionToJson(writer, sort_info.expression);
    writer.Key("order");
    writer.String(sort_info.order == SortInfo::Order::ASCENDING ? "ASC"
                                                                : "DESC");
    writer.Key("null_order");
    writer.String(sort_info.null_order == SortInfo::NullOrder::FIRST
                      ? "NULLS_FIRST"
                      : "NULLS_LAST");
    writer.EndObject();
  }
  writer.EndArray();
}

void limitToJson(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                 std::shared_ptr<const RelLimit> limit) {
  writer.Key("n");
  writer.Uint64(limit->n());
  if (limit->offset() > 0) {
    writer.Key("offset");
    writer.Uint64(limit->offset());
  }
}

void dispatchRel(rapidjson::Writer<rapidjson::StringBuffer> &writer,
                 std::shared_ptr<const Rel> rel) {
  writer.StartObject();
  writer.Key("rel_op");
  if (auto input_source = std::dynamic_pointer_cast<const RelInputSource>(rel);
      input_source) {
    writer.String("InputSource");
    inputSourceToJson(writer, input_source);
  } else if (auto filter = std::dynamic_pointer_cast<const RelFilter>(rel);
             filter) {
    writer.String("Filter");
    filterToJson(writer, filter);
  } else if (auto u = std::dynamic_pointer_cast<const RelUnion>(rel); u) {
    writer.String("Union");
    unionToJson(writer, u);
  } else if (auto ua = std::dynamic_pointer_cast<const RelUnionAll>(rel); ua) {
    writer.String("UnionAll");
    unionAllToJson(writer, ua);
  } else if (auto hj = std::dynamic_pointer_cast<const RelHashJoin>(rel); hj) {
    writer.String("HashJoin");
    hashJoinToJson(writer, hj);
  } else if (auto project = std::dynamic_pointer_cast<const RelProject>(rel);
             project) {
    writer.String("Project");
    projectToJson(writer, project);
  } else if (auto aggregate =
                 std::dynamic_pointer_cast<const RelAggregate>(rel);
             aggregate) {
    writer.String("Aggregate");
    aggregateToJson(writer, aggregate);
  } else if (auto sort = std::dynamic_pointer_cast<const RelSort>(rel); sort) {
    writer.String("Sort");
    sortToJson(writer, sort);
  } else if (auto limit = std::dynamic_pointer_cast<const RelLimit>(rel);
             limit) {
    writer.String("Limit");
    limitToJson(writer, limit);
  } else {
    CURA_FAIL("Unknown Rel type " + rel->toString());
  }
  writer.EndObject();
}

} // namespace detail

std::string toJson(std::shared_ptr<const Rel> rel) {
  std::vector<std::shared_ptr<const Rel>> rels;
  detail::flattenRel(rel, rels);

  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  writer.StartObject();
  writer.Key("rels");
  writer.StartArray();
  for (const auto &r : rels) {
    detail::dispatchRel(writer, r);
  }
  writer.EndArray();
  writer.EndObject();

  return buf.GetString();
}

} // namespace cura::test::relational