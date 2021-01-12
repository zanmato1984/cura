#pragma once

#include "cura/expression/expressions.h"

namespace cura::expression {

#define APPLY_FOR_AGGREGATION_OPERATORS(ACTION)                                \
  ACTION(SUM)                                                                  \
  ACTION(PRODUCT)                                                              \
  ACTION(MIN)                                                                  \
  ACTION(MAX)                                                                  \
  ACTION(COUNT_VALID)                                                          \
  ACTION(COUNT_ALL)                                                            \
  ACTION(ANY)                                                                  \
  ACTION(ALL)                                                                  \
  ACTION(SUM_OF_SQUARES)                                                       \
  ACTION(MEAN)                                                                 \
  ACTION(MEDIAN)                                                               \
  ACTION(QUANTILE)                                                             \
  ACTION(ARGMAX)                                                               \
  ACTION(ARGMIN)                                                               \
  ACTION(NUNIQUE)                                                              \
  ACTION(NTH_ELEMENT)                                                          \
  // ACTION(ROW_NUMBER)                                                           \
  // ACTION(COLLECT)                                                              \
  // ACTION(VARIANCE)                                                             \
  // ACTION(STD)                                                                  \
  // ACTION(PTX)                                                                  \
  // ACTION(CUDA)

#define DEF_AGGREGATION_OPERATOR_ENUM(OP) OP,

enum class AggregationOperator : int32_t {
  APPLY_FOR_AGGREGATION_OPERATORS(DEF_AGGREGATION_OPERATOR_ENUM)
};

#undef DEF_AGGREGATION_OPERATOR_ENUM

inline std::string aggregationOperatorToString(AggregationOperator op) {
#define AGGREGATION_OP_CASE(OP)                                                \
  case AggregationOperator::OP:                                                \
    return CURA_STRINGIFY(OP);

  switch (op) {
    APPLY_FOR_AGGREGATION_OPERATORS(AGGREGATION_OP_CASE);
  default:
    CURA_FAIL("Unknown aggregation op " +
              std::to_string(static_cast<int32_t>(op)));
  }

#undef AGGREGATION_OP_CASE
}

inline AggregationOperator aggregationOperatorFromString(const std::string &s) {
#define AGGREGATION_OP_CASE(OP)                                                \
  if (s == CURA_STRINGIFY(OP)) {                                               \
    return AggregationOperator::OP;                                            \
  }

  APPLY_FOR_AGGREGATION_OPERATORS(AGGREGATION_OP_CASE)

  CURA_FAIL("Invalid aggregation operator: " + s);

#undef UNARY_OP_CASE
}

struct Aggregation : public Op {
  Aggregation(AggregationOperator aggregation_operator_,
              std::shared_ptr<const Expression> operand, DataType data_type)
      : Op({operand}, data_type), aggregation_operator(aggregation_operator_) {
    CURA_ASSERT(operand, "Invalid operand for Aggregation");
  }

  const DataType &dataType() const override { return data_type; }

  std::string name() const override { return "Aggregation"; }

  std::string toString() const override {
    return aggregationOperatorToString(aggregation_operator) + "(" +
           operands_[0]->toString() + ")";
  }

  AggregationOperator aggregationOperator() const {
    return aggregation_operator;
  }

protected:
  AggregationOperator aggregation_operator;
};

struct NthElement : public Aggregation {
  NthElement(std::shared_ptr<const Expression> operand, DataType data_type,
             int64_t n)
      : Aggregation(AggregationOperator::NTH_ELEMENT, {operand}, data_type),
        n_(n) {}

  std::string toString() const override {
    return aggregationOperatorToString(aggregation_operator) + "(" +
           std::to_string(n_) + ", " + operands_[0]->toString() + ")";
  }

  int64_t n() const { return n_; }

private:
  int64_t n_;
};

}; // namespace cura::expression
