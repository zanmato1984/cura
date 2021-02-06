#pragma once

#include "cura/expression/aggregation.h"
#include "cura/expression/binary_op.h"
#include "cura/expression/literal.h"
#include "cura/expression/ti_unary_op.h"
#include "cura/expression/unary_op.h"
#include "utilities/rel_helper.h"

#include <functional>
#include <memory>

using cura::SourceId;
using cura::expression::Aggregation;
using cura::expression::AggregationOperator;
using cura::expression::BinaryOp;
using cura::expression::BinaryOperator;
using cura::expression::ColumnIdx;
using cura::expression::ColumnRef;
using cura::expression::Expression;
using cura::expression::Literal;
using cura::expression::TiUnaryOp;
using cura::expression::UnaryOp;
using cura::relational::BuildSide;
using cura::relational::JoinType;
using cura::relational::Rel;
using cura::relational::RelAggregate;
using cura::relational::RelFilter;
using cura::relational::RelHashJoin;
using cura::relational::RelInputSource;
using cura::relational::RelLimit;
using cura::relational::RelProject;
using cura::relational::RelSort;
using cura::relational::RelUnion;
using cura::relational::RelUnionAll;
using cura::relational::SortInfo;
using cura::test::relational::makeRel;
using cura::type::DataType;
using cura::type::Schema;
using cura::type::TypeId;

inline std::shared_ptr<const Rel> makeRelInputSource(SourceId source_id,
                                                     bool nullable = false) {
  return makeRel<RelInputSource>(source_id,
                                 Schema{DataType::int32Type(nullable)});
}

template <typename U> inline std::shared_ptr<const Rel> makeSingleLevelUnion() {
  return makeRel<U>(std::vector<std::shared_ptr<const Rel>>{
      makeRelInputSource(100), makeRelInputSource(200)});
}

template <typename U1, typename U2>
inline std::shared_ptr<const Rel> makeImbalancedDoubleLevelUnion() {
  auto u = makeRel<U1>(std::vector<std::shared_ptr<const Rel>>{
      makeRelInputSource(100), makeRelInputSource(200)});
  return makeRel<U2>(
      std::vector<std::shared_ptr<const Rel>>{u, makeRelInputSource(300)});
}

template <typename U1, typename U2, typename U3>
inline std::shared_ptr<const Rel> makeBalancedDoubleLevelUnion() {
  auto u1 = makeRel<U1>(std::vector<std::shared_ptr<const Rel>>{
      makeRelInputSource(100), makeRelInputSource(200)});
  auto u2 = makeRel<U2>(std::vector<std::shared_ptr<const Rel>>{
      makeRelInputSource(300), makeRelInputSource(400)});
  return makeRel<U3>(std::vector<std::shared_ptr<const Rel>>{u1, u2});
}

inline std::shared_ptr<const Expression>
makeFilterCondition(ColumnIdx column_idx, int32_t value) {
  auto column_ref = std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto literal = std::make_shared<const Literal>(TypeId::INT32, value);
  return std::make_shared<const BinaryOp>(BinaryOperator::EQUAL, column_ref,
                                          literal, DataType::bool8Type());
}

inline std::shared_ptr<const Expression>
makeJoinCondition(ColumnIdx probe_key, ColumnIdx build_key,
                  const DataType &data_type) {
  auto column_ref_left =
      std::make_shared<const ColumnRef>(probe_key, data_type);
  auto column_ref_right =
      std::make_shared<const ColumnRef>(build_key, data_type);
  return std::make_shared<const BinaryOp>(
      BinaryOperator::EQUAL, column_ref_left, column_ref_right,
      DataType::bool8Type(data_type.nullable));
}

inline std::shared_ptr<const Expression>
makeConjunction(std::shared_ptr<const Expression> left,
                std::shared_ptr<const Expression> right) {
  return std::make_shared<const BinaryOp>(
      BinaryOperator::LOGICAL_AND, left, right,
      DataType::bool8Type(left->dataType().nullable ||
                          right->dataType().nullable));
}

inline void iterateSingleLevelUnionTypes(
    std::function<void(std::shared_ptr<const Rel>)> f) {
  f(makeSingleLevelUnion<RelUnion>());
  f(makeSingleLevelUnion<RelUnionAll>());
}

inline void iterateImbalancedDoubleLevelUnionTypes(
    std::function<void(std::shared_ptr<const Rel>)> f) {
  f(makeImbalancedDoubleLevelUnion<RelUnion, RelUnion>());
  f(makeImbalancedDoubleLevelUnion<RelUnionAll, RelUnion>());
  f(makeImbalancedDoubleLevelUnion<RelUnion, RelUnionAll>());
  f(makeImbalancedDoubleLevelUnion<RelUnionAll, RelUnionAll>());
}

inline void iterateBalancedDoubleLevelUnionTypes(
    std::function<void(std::shared_ptr<const Rel>)> f) {
  f(makeBalancedDoubleLevelUnion<RelUnion, RelUnion, RelUnion>());
  f(makeBalancedDoubleLevelUnion<RelUnionAll, RelUnion, RelUnion>());
  f(makeBalancedDoubleLevelUnion<RelUnion, RelUnionAll, RelUnion>());
  f(makeBalancedDoubleLevelUnion<RelUnion, RelUnion, RelUnionAll>());
  f(makeBalancedDoubleLevelUnion<RelUnion, RelUnionAll, RelUnionAll>());
  f(makeBalancedDoubleLevelUnion<RelUnionAll, RelUnion, RelUnionAll>());
  f(makeBalancedDoubleLevelUnion<RelUnionAll, RelUnionAll, RelUnion>());
  f(makeBalancedDoubleLevelUnion<RelUnionAll, RelUnionAll, RelUnionAll>());
}

inline void
iterateHashJoins(std::function<void(std::shared_ptr<const Rel>)> f) {
  {
    auto hash_join = makeRel<RelHashJoin>(
        JoinType::INNER, makeRelInputSource(100), makeRelInputSource(200),
        BuildSide::RIGHT, makeJoinCondition(0, 1, DataType::int32Type()));
    f(hash_join);
  }
  {
    auto hash_join1 = makeRel<RelHashJoin>(
        JoinType::INNER, makeRelInputSource(100), makeRelInputSource(200),
        BuildSide::LEFT, makeJoinCondition(0, 1, DataType::int32Type()));
    auto hash_join2 = makeRel<RelHashJoin>(
        JoinType::LEFT, hash_join1, makeRelInputSource(300), BuildSide::RIGHT,
        makeJoinCondition(0, 2, DataType::int32Type()));
    f(hash_join2);
  }
  {
    auto hash_join1 = makeRel<RelHashJoin>(
        JoinType::INNER, makeRelInputSource(100), makeRelInputSource(200),
        BuildSide::RIGHT, makeJoinCondition(0, 1, DataType::int32Type()));
    auto hash_join2 = makeRel<RelHashJoin>(
        JoinType::LEFT, makeRelInputSource(300), hash_join1, BuildSide::LEFT,
        makeJoinCondition(0, 2, DataType::int32Type()));
    f(hash_join2);
  }
}

inline void
iterateInnerJoins(std::function<void(std::shared_ptr<const Rel>)> f) {
  auto make_join = [&](BuildSide build_side) {
    auto s0 = makeRel<RelInputSource>(
        100, Schema{DataType::int32Type(), DataType::stringType()});
    auto s1 = makeRel<RelInputSource>(
        200, Schema{DataType::int32Type(), DataType::stringType()});
    auto cond0 = makeJoinCondition(0, 2, DataType::int32Type());
    auto cond1 = makeJoinCondition(1, 3, DataType::stringType());
    auto cond = makeConjunction(cond0, cond1);
    auto hash_join =
        makeRel<RelHashJoin>(JoinType::INNER, s0, s1, build_side, cond);
    return std::make_shared<RelProject>(
        hash_join,
        std::vector<std::shared_ptr<const Expression>>{
            std::make_shared<const ColumnRef>(1, DataType::stringType())});
  };

  {
    auto join = make_join(BuildSide::RIGHT);
    f(join);
  }
  {
    auto join = make_join(BuildSide::LEFT);
    f(join);
  }
}

inline void
iterateHashJoinWithFilters(std::function<void(std::shared_ptr<const Rel>)> f) {
  {
    auto hash_join = makeRel<RelHashJoin>(
        JoinType::INNER,
        makeRel<RelFilter>(makeRelInputSource(100), makeFilterCondition(0, 42)),
        makeRelInputSource(200), BuildSide::LEFT,
        makeJoinCondition(0, 1, DataType::int32Type()));
    f(hash_join);
  }
  {
    auto hash_join1 = makeRel<RelHashJoin>(
        JoinType::INNER, makeRelInputSource(100), makeRelInputSource(200),
        BuildSide::RIGHT, makeJoinCondition(0, 1, DataType::int32Type()));
    auto filter = makeRel<RelFilter>(hash_join1, makeFilterCondition(0, 42));
    auto hash_join2 = makeRel<RelHashJoin>(
        JoinType::LEFT, filter, makeRelInputSource(300), BuildSide::RIGHT,
        makeJoinCondition(0, 2, DataType::int32Type()));
    f(hash_join2);
  }
  {
    auto hash_join1 = makeRel<RelHashJoin>(
        JoinType::INNER,
        makeRel<RelFilter>(makeRelInputSource(100), makeFilterCondition(0, 42)),
        makeRelInputSource(200), BuildSide::LEFT,
        makeJoinCondition(0, 1, DataType::int32Type()));
    auto hash_join2 = makeRel<RelHashJoin>(
        JoinType::LEFT, makeRelInputSource(300), hash_join1, BuildSide::LEFT,
        makeJoinCondition(0, 2, DataType::int32Type()));
    auto filter = makeRel<RelFilter>(hash_join2, makeFilterCondition(0, 42));
    f(filter);
  }
}

inline void
hashJoinWithTwoUnions(std::function<void(std::shared_ptr<const Rel>)> f) {
  auto makeInputSource = [](SourceId id) {
    return makeRel<RelInputSource>(
        id, Schema{DataType::int64Type(true), DataType::stringType(true)});
  };
  auto s1 = makeInputSource(100);
  auto s2 = makeInputSource(200);
  auto s3 = makeInputSource(300);
  auto s4 = makeInputSource(400);
  auto u1 = makeRel<RelUnion>(std::vector<std::shared_ptr<const Rel>>{s1, s2});
  auto u2 = makeRel<RelUnion>(std::vector<std::shared_ptr<const Rel>>{s3, s4});
  auto cond = makeJoinCondition(1, 3, DataType::stringType(true));
  auto hj =
      makeRel<RelHashJoin>(JoinType::INNER, u1, u2, BuildSide::LEFT, cond);
  f(hj);
}

inline void simpleProject(std::function<void(std::shared_ptr<const Rel>)> f) {
  auto input_source = makeRel<RelInputSource>(
      100, Schema{DataType::int32Type(), DataType::int32Type(),
                  DataType::int32Type(), DataType::int32Type()});
  auto e0 = std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto e1 = std::make_shared<const BinaryOp>(
      BinaryOperator::ADD, e0,
      std::make_shared<const Literal>(TypeId::INT32, 42),
      DataType::int32Type());
  auto project = std::make_shared<RelProject>(
      input_source, std::vector<std::shared_ptr<const Expression>>{e0, e1});
  f(project);
}

inline void simpleAggregate(std::function<void(std::shared_ptr<const Rel>)> f) {
  auto input_source = makeRel<RelInputSource>(
      100, Schema{DataType::int32Type(), DataType::int32Type(),
                  DataType::stringType(), DataType::int32Type()});
  auto key0 = std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto key1 = std::make_shared<const ColumnRef>(2, DataType::stringType());
  auto agg0 = std::make_shared<const Aggregation>(
      AggregationOperator::SUM,
      std::make_shared<const ColumnRef>(1, DataType::int32Type()),
      DataType::int64Type());
  auto agg1 = std::make_shared<const Aggregation>(
      AggregationOperator::COUNT_ALL,
      std::make_shared<const ColumnRef>(3, DataType::int32Type()),
      DataType::int64Type());
  auto aggregate = std::make_shared<RelAggregate>(
      input_source, std::vector<std::shared_ptr<const Expression>>{key0, key1},
      std::vector<std::shared_ptr<const Expression>>{agg0, agg1});
  f(aggregate);
}

inline void
aggregateLiteral(std::function<void(std::shared_ptr<const Rel>)> f) {
  auto make_count = [&](std::shared_ptr<const Literal> literal) {
    auto input_source =
        makeRel<RelInputSource>(100, Schema{DataType::int32Type()});
    auto project = makeRel<RelProject>(
        input_source, std::vector<std::shared_ptr<const Expression>>{literal});
    auto agg = std::make_shared<const Aggregation>(
        AggregationOperator::COUNT_VALID,
        std::make_shared<const ColumnRef>(0, literal->dataType()),
        DataType::int64Type());
    return std::make_shared<RelAggregate>(
        project, std::vector<std::shared_ptr<const Expression>>{},
        std::vector<std::shared_ptr<const Expression>>{agg});
  };

  {
    auto aggregate =
        make_count(std::make_shared<const Literal>(TypeId::INT32, 1));
    f(aggregate);
  }
  {
    auto aggregate = make_count(std::make_shared<const Literal>(TypeId::INT32));
    f(aggregate);
  }
}

inline void
projectAggregate(std::function<void(std::shared_ptr<const Rel>)> f) {
  auto input_source = makeRel<RelInputSource>(
      100, Schema{DataType::int32Type(), DataType::int32Type(),
                  DataType::int32Type(), DataType::stringType()});

  auto project = [&] {
    auto c0 = std::make_shared<const ColumnRef>(0, DataType::int32Type());
    auto c1 = std::make_shared<const ColumnRef>(1, DataType::int32Type());
    auto c2 = std::make_shared<const ColumnRef>(2, DataType::int32Type());
    auto c3 = std::make_shared<const ColumnRef>(3, DataType::stringType());

    auto p0 = std::make_shared<const BinaryOp>(
        BinaryOperator::ADD, c0,
        std::make_shared<const BinaryOp>(
            BinaryOperator::ADD, c1,
            std::make_shared<const Literal>(TypeId::INT32, 42),
            DataType::int32Type()),
        DataType::int32Type());
    auto p1 = std::make_shared<const BinaryOp>(
        BinaryOperator::ADD, c1,
        std::make_shared<const BinaryOp>(
            BinaryOperator::ADD, c2,
            std::make_shared<const Literal>(TypeId::INT32, 42),
            DataType::int32Type()),
        DataType::int32Type());
    auto p2 = c3;
    return std::make_shared<RelProject>(
        input_source,
        std::vector<std::shared_ptr<const Expression>>{p0, p1, p2});
  }();

  auto aggregate = [&]() {
    auto key0 = std::make_shared<const ColumnRef>(2, DataType::stringType());
    auto agg0 = std::make_shared<const Aggregation>(
        AggregationOperator::SUM,
        std::make_shared<const ColumnRef>(0, DataType::int32Type()),
        DataType::int64Type());
    auto agg1 = std::make_shared<const Aggregation>(
        AggregationOperator::MEAN,
        std::make_shared<const ColumnRef>(1, DataType::int32Type()),
        DataType::float64Type());
    return std::make_shared<RelAggregate>(
        project, std::vector<std::shared_ptr<const Expression>>{key0},
        std::vector<std::shared_ptr<const Expression>>{agg0, agg1});
  }();
  f(aggregate);
}

inline void simpleSort(std::function<void(std::shared_ptr<const Rel>)> f) {
  auto input_source = makeRel<RelInputSource>(
      100, Schema{DataType::int32Type(true), DataType::int32Type(),
                  DataType::stringType(true)});
  auto c0 = std::make_shared<const ColumnRef>(0, DataType::int32Type(true));
  auto c2 = std::make_shared<const ColumnRef>(2, DataType::stringType(true));
  auto sort = std::make_shared<RelSort>(
      input_source,
      std::vector<SortInfo>{
          {c0, SortInfo::Order::ASCENDING, SortInfo::NullOrder::FIRST},
          {c2, SortInfo::Order::DESCENDING, SortInfo::NullOrder::LAST}});
  f(sort);
}

inline void simpleLimit(std::function<void(std::shared_ptr<const Rel>)> f,
                        size_t offset, size_t n) {
  auto input_source =
      makeRel<RelInputSource>(100, Schema{DataType::int32Type()});
  auto limit = std::make_shared<RelLimit>(input_source, offset, n);
  f(limit);
}

inline void topN(std::function<void(std::shared_ptr<const Rel>)> f, size_t n) {
  auto input_source =
      makeRel<RelInputSource>(100, Schema{DataType::int32Type()});
  auto c = std::make_shared<const ColumnRef>(0, DataType::int32Type());
  auto sort = std::make_shared<RelSort>(
      input_source, std::vector<SortInfo>{{c, SortInfo::Order::ASCENDING,
                                           SortInfo::NullOrder::FIRST}});
  auto limit = std::make_shared<RelLimit>(sort, 0, n);
  f(limit);
}
