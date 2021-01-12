#pragma once

#include "cura/common/errors.h"
#include "cura/common/types.h"
#include "cura/expression/expressions.h"

#include <algorithm>
#include <memory>
#include <sstream>
#include <string>

namespace cura::relational {

using cura::expression::ColumnRef;
using cura::expression::Expression;
using cura::type::DataType;
using cura::type::Schema;

struct Rel {
  std::vector<std::shared_ptr<const Rel>> inputs;

  explicit Rel(std::vector<std::shared_ptr<const Rel>> inputs_)
      : inputs(std::move(inputs_)) {}

  virtual ~Rel() = default;

  virtual const Schema &output() const = 0;

  virtual std::string name() const = 0;

  virtual std::string toString() const { return name(); }
};

struct RelInputSource : public Rel {

  RelInputSource(SourceId source_id_, Schema schema)
      : Rel({}), source_id(source_id_), schema_(std::move(schema)) {}

  const Schema &output() const override { return schema_; }

  std::string name() const override { return "InputSource"; }

  std::string toString() const override {
    return Rel::toString() + "(" + std::to_string(source_id) + ", schema: [" +
           std::accumulate(schema_.begin() + 1, schema_.end(),
                           schema_[0].toString(),
                           [](const auto &all, const auto &data_type) {
                             return all + ", " + data_type.toString();
                           }) +
           +"])";
  }

  SourceId sourceId() const { return source_id; }

  const Schema &schema() const { return schema_; }

private:
  SourceId source_id;
  Schema schema_;
};

struct RelFilter : public Rel {
  RelFilter(std::shared_ptr<const Rel> input,
            std::shared_ptr<const Expression> cond_)
      : Rel({input}), cond(cond_) {
    CURA_ASSERT(input, "Invalid RelFilter input");
    CURA_ASSERT(cond, "Invalid RelFilter condition");
  }

  const Schema &output() const override { return inputs[0]->output(); }

  std::string name() const override { return "Filter"; }

  std::string toString() const override {
    return Rel::toString() + "(" + cond->toString() + ")";
  }

  std::shared_ptr<const Expression> condition() const { return cond; }

private:
  std::shared_ptr<const Expression> cond;
};

struct RelUnion : public Rel {
  RelUnion(std::vector<std::shared_ptr<const Rel>> inputs_)
      : Rel(std::move(inputs_)) {
    CURA_ASSERT(inputs.size() > 1, "Invalid RelUnion input size");

    const auto &schema = inputs[0]->output();
    std::for_each(inputs.begin() + 1, inputs.end(),
                  [&schema](const auto &input) {
                    CURA_ASSERT(input, "Invalid RelUnion input");
                    CURA_ASSERT(input->output() == schema,
                                "Mismatched RelUnion input schema");
                  });
  }

  const Schema &output() const override { return inputs[0]->output(); }

  std::string name() const override { return "Union"; }
};

struct RelUnionAll : public Rel {
  RelUnionAll(std::vector<std::shared_ptr<const Rel>> inputs_)
      : Rel(std::move(inputs_)) {
    CURA_ASSERT(inputs.size() > 1, "Invalid RelUnionAll input size");

    const auto &schema = inputs[0]->output();
    std::for_each(inputs.begin() + 1, inputs.end(),
                  [&schema](const auto &input) {
                    CURA_ASSERT(input, "Invalid RelUnionAll input");
                    CURA_ASSERT(input->output() == schema,
                                "Mismatched RelUnionAll input schema");
                  });
  }

  const Schema &output() const override { return inputs[0]->output(); }

  std::string name() const override { return "UnionAll"; }
};

#define APPLY_FOR_JOIN_TYPES(ACTION)                                           \
  ACTION(INNER)                                                                \
  ACTION(LEFT)                                                                 \
  ACTION(FULL)                                                                 \
  ACTION(LEFT_SEMI)                                                            \
  ACTION(LEFT_ANTI)

#define DEF_JOIN_TYPE_ENUM(TYPE) TYPE,

enum class JoinType : int32_t { APPLY_FOR_JOIN_TYPES(DEF_JOIN_TYPE_ENUM) };

#undef DEF_JOIN_TYPE_ENUM

inline std::string joinTypeToString(JoinType join_type) {
#define JOIN_TYPE_CASE(TYPE)                                                   \
  case JoinType::TYPE:                                                         \
    return CURA_STRINGIFY(TYPE);

  switch (join_type) {
    APPLY_FOR_JOIN_TYPES(JOIN_TYPE_CASE);
  default:
    CURA_FAIL("Unknown join type " +
              std::to_string(static_cast<int32_t>(join_type)));
  }

#undef JOIN_TYPE_CASE
}

inline JoinType joinTypeFromString(const std::string &s) {
#define JOIN_TYPE_CASE(TYPE)                                                   \
  if (s == CURA_STRINGIFY(TYPE)) {                                             \
    return JoinType::TYPE;                                                     \
  }

  APPLY_FOR_JOIN_TYPES(JOIN_TYPE_CASE)

  CURA_FAIL("Invalid join type: " + s);

#undef JOIN_TYPE_CASE
}

enum class BuildSide : int32_t { LEFT, RIGHT };

inline std::string buildSideToString(BuildSide build_side) {
  return build_side == BuildSide::LEFT ? "LEFT" : "RIGHT";
}

inline BuildSide buildSideFromString(const std::string &s) {
  if (s == "LEFT") {
    return BuildSide::LEFT;
  }

  if (s == "RIGHT") {
    return BuildSide::RIGHT;
  }

  CURA_FAIL("Invalid build gtype: " + s);
}

struct RelHashJoin : public Rel {
  RelHashJoin(JoinType join_type_, std::shared_ptr<const Rel> left,
              std::shared_ptr<const Rel> right, BuildSide build_side_,
              std::shared_ptr<const Expression> cond_)
      : Rel({left, right}), join_type(join_type_), build_side(build_side_),
        cond(cond_) {
    CURA_ASSERT(cond, "Invalid RelHashJoin condition");
    const auto &left_output = left->output();
    const auto &right_output = right->output();
    if (join_type == JoinType::FULL) {
      schema.resize(left_output.size() + right_output.size());
      std::transform(left_output.begin(), left_output.end(), schema.begin(),
                     [](const auto &data_type) {
                       return DataType(data_type.type_id, true);
                     });
      std::transform(right_output.begin(), right_output.end(),
                     schema.begin() + left_output.size(),
                     [](const auto &data_type) {
                       return DataType(data_type.type_id, true);
                     });
    } else if (join_type == JoinType::LEFT) {
      schema.insert(schema.end(), left_output.begin(), left_output.end());
      schema.resize(left_output.size() + right_output.size());
      std::transform(right_output.begin(), right_output.end(),
                     schema.begin() + left_output.size(),
                     [](const auto &data_type) {
                       return DataType(data_type.type_id, true);
                     });
    } else {
      schema.insert(schema.end(), left_output.begin(), left_output.end());
      schema.insert(schema.end(), right_output.begin(), right_output.end());
    }
  }

  const Schema &output() const override { return schema; }

  std::string name() const override { return "HashJoin"; }

  std::string toString() const override {
    std::stringstream ss;
    ss << Rel::toString() + "(" + joinTypeToString(join_type);
    if (join_type == JoinType::INNER) {
      ss << ", build_side: " << buildSideToString(build_side);
    }
    ss << ", cond: " << cond->toString() << ")";
    return ss.str();
  }

  JoinType joinType() const { return join_type; }

  BuildSide buildSide() const { return build_side; }

  std::shared_ptr<const Rel> left() const { return inputs[0]; }

  std::shared_ptr<const Rel> right() const { return inputs[1]; }

  std::shared_ptr<const Expression> condition() const { return cond; }

private:
  JoinType join_type;
  Schema schema;
  BuildSide build_side;
  std::shared_ptr<const Expression> cond;
};

struct RelHashJoinBuild : public Rel {
  RelHashJoinBuild(std::shared_ptr<const Rel> input,
                   std::vector<std::shared_ptr<const ColumnRef>> keys_)
      : Rel({input}), keys(std::move(keys_)) {
    CURA_ASSERT(input, "Invalid RelHashJoinBuild input");
    CURA_ASSERT(!keys.empty(), "Empty keys for RelHashJoinBuild");
  }

  const Schema &output() const override { return inputs[0]->output(); }

  std::string name() const override { return "HashJoinBuild"; }

  const std::vector<std::shared_ptr<const ColumnRef>> &buildKeys() const {
    return keys;
  }

private:
  std::vector<std::shared_ptr<const ColumnRef>> keys;
};

struct RelHashJoinProbe : public Rel {
  RelHashJoinProbe(JoinType join_type_, std::shared_ptr<const Rel> probe,
                   std::shared_ptr<const Rel> build,
                   std::vector<std::shared_ptr<const ColumnRef>> keys_,
                   Schema schema_, BuildSide build_side_)
      : Rel({probe, build}), join_type(join_type_), keys(std::move(keys_)),
        schema(std::move(schema_)), build_side(build_side_) {
    CURA_ASSERT(inputs[0], "Invalid probe side of RelHashJoinProbe");
    CURA_ASSERT(std::dynamic_pointer_cast<const RelHashJoinBuild>(inputs[1]),
                "Invalid build side of RelHashJoinProbe");
    CURA_ASSERT(!keys.empty(), "Empty keys for RelHashJoinProbe");
  }

  const Schema &output() const override { return schema; }

  std::string name() const override { return "HashJoinProbe"; }

  JoinType joinType() const { return join_type; }

  std::shared_ptr<const RelHashJoinBuild> buildInput() const {
    return std::dynamic_pointer_cast<const RelHashJoinBuild>(inputs[1]);
  }

  const std::vector<std::shared_ptr<const ColumnRef>> &probeKeys() const {
    return keys;
  }

  BuildSide buildSide() const { return build_side; }

private:
  Schema schema;
  JoinType join_type;
  BuildSide build_side;
  std::vector<std::shared_ptr<const ColumnRef>> keys;
};

struct RelProject : public Rel {
  RelProject(std::shared_ptr<const Rel> input,
             std::vector<std::shared_ptr<const Expression>> expressions)
      : Rel({input}), expressions_(std::move(expressions)) {
    CURA_ASSERT(input, "Invalid RelProject input");
    CURA_ASSERT(!expressions_.empty(), "Empty RelProject expressions");

    schema.resize(expressions_.size());
    std::transform(expressions_.begin(), expressions_.end(), schema.begin(),
                   [](const auto &e) { return e->dataType(); });
  }

  const Schema &output() const override { return schema; }

  std::string name() const override { return "Project"; }

  std::string toString() const override {
    return Rel::toString() + "(" +
           std::accumulate(expressions_.begin() + 1, expressions_.end(),
                           expressions_[0]->toString(),
                           [](const auto &all, const auto &e) {
                             return all + ", " + e->toString();
                           }) +
           ")";
  }

  const std::vector<std::shared_ptr<const Expression>> &expressions() const {
    return expressions_;
  }

private:
  Schema schema;
  std::vector<std::shared_ptr<const Expression>> expressions_;
};

struct RelAggregate : public Rel {
  RelAggregate(std::shared_ptr<const Rel> input,
               std::vector<std::shared_ptr<const Expression>> groups,
               std::vector<std::shared_ptr<const Expression>> aggregations)
      : Rel({input}), groups_(std::move(groups)),
        aggregations_(std::move(aggregations)) {
    CURA_ASSERT(input, "Invalid RelAggregate input");
    CURA_ASSERT(!aggregations_.empty(), "Empty RelAggregate aggregates");

    schema.resize(groups_.size() + aggregations_.size());
    std::transform(groups_.begin(), groups_.end(), schema.begin(),
                   [](const auto &group) { return group->dataType(); });
    std::transform(aggregations_.begin(), aggregations_.end(),
                   schema.begin() + groups_.size(),
                   [](const auto &aggregate) { return aggregate->dataType(); });
  }

  const Schema &inputSchema() const { return inputs[0]->output(); }

  const Schema &output() const override { return schema; }

  std::string name() const override { return "Aggregate"; }

  std::string toString() const override {
    std::stringstream ss;
    if (!groups_.empty()) {
      ss << "groups: ["
         << std::accumulate(groups_.begin() + 1, groups_.end(),
                            groups_[0]->toString(),
                            [](const auto &all, const auto &group) {
                              return all + ", " + group->toString();
                            }) +
                "], ";
    }
    ss << "aggregates: ["
       << std::accumulate(aggregations_.begin() + 1, aggregations_.end(),
                          aggregations_[0]->toString(),
                          [](const auto &all, const auto &aggregation) {
                            return all + ", " + aggregation->toString();
                          }) +
              "]";
    return Rel::toString() + "(" + ss.str() + ")";
  }

  const std::vector<std::shared_ptr<const Expression>> &groups() const {
    return groups_;
  }

  const std::vector<std::shared_ptr<const Expression>> &aggregations() const {
    return aggregations_;
  }

private:
  Schema schema;
  std::vector<std::shared_ptr<const Expression>> groups_;
  std::vector<std::shared_ptr<const Expression>> aggregations_;
};

struct SortInfo {
  enum class Order : bool { ASCENDING, DESCENDING };

  enum class NullOrder : bool { FIRST, LAST };

  std::shared_ptr<const Expression> expression;
  Order order;
  NullOrder null_order;

  std::string toString() const {
    return expression->toString() + " " +
           (order == Order::ASCENDING ? "ASC" : "DESC") + " NULL_" +
           (null_order == NullOrder::FIRST ? "FIRST" : "LAST");
  }
};

struct RelSort : public Rel {
  RelSort(std::shared_ptr<const Rel> input, std::vector<SortInfo> sort_infos_)
      : Rel({input}), sort_infos(std::move(sort_infos_)) {
    CURA_ASSERT(input, "Invalid RelSort input");
    CURA_ASSERT(!sort_infos.empty(), "Empty RelSort sort infos");
  }

  const Schema &output() const override { return inputs[0]->output(); }

  std::string name() const override { return "Sort"; }

  std::string toString() const override {
    return Rel::toString() + "([" +
           std::accumulate(sort_infos.begin() + 1, sort_infos.end(),
                           sort_infos[0].toString(),
                           [](const auto &all, const auto &sort_info) {
                             return all + ", " + sort_info.toString();
                           }) +
           "])";
  }

  const std::vector<SortInfo> &sortInfos() const { return sort_infos; }

private:
  std::vector<SortInfo> sort_infos;
};

struct RelLimit : public Rel {
  RelLimit(std::shared_ptr<const Rel> input, size_t offset, size_t n)
      : Rel({input}), offset_(offset), n_(n) {
    CURA_ASSERT(input, "Invalid RelLimit input");
  }

  const Schema &output() const override { return inputs[0]->output(); }

  std::string name() const override { return "Limit"; }

  std::string toString() const override {
    return Rel::toString() + "(" +
           (offset_ == 0 ? "" : (std::to_string(offset_) + ", ")) +
           std::to_string(n_) + ")";
  }

  size_t offset() const { return offset_; }

  size_t n() const { return n_; }

private:
  size_t offset_;
  size_t n_;
};

} // namespace cura::relational
