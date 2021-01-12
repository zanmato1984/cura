#pragma once

#include "cura/expression/expressions.h"

namespace cura::expression {

#define APPLY_FOR_BINARY_OPERATORS(ACTION)                                     \
  ACTION(ADD, +)                                                               \
  ACTION(SUB, -)                                                               \
  ACTION(MUL, *)                                                               \
  ACTION(DIV, /)                                                               \
  ACTION(TRUE_DIV, /)                                                          \
  ACTION(FLOOR_DIV, /)                                                         \
  ACTION(MOD, %)                                                               \
  ACTION(PYMOD, %)                                                             \
  ACTION(POW, )                                                                \
  ACTION(EQUAL, =)                                                             \
  ACTION(NOT_EQUAL, !=)                                                        \
  ACTION(LESS, <)                                                              \
  ACTION(GREATER, >)                                                           \
  ACTION(LESS_EQUAL, <=)                                                       \
  ACTION(GREATER_EQUAL, >=)                                                    \
  ACTION(BITWISE_AND, &)                                                       \
  ACTION(BITWISE_OR, |)                                                        \
  ACTION(BITWISE_XOR, ^)                                                       \
  ACTION(LOGICAL_AND, &&)                                                      \
  ACTION(LOGICAL_OR, ||)                                                       \
  ACTION(COALESCE, )                                                           \
  ACTION(GENERIC_BINARY, )                                                     \
  ACTION(SHIFT_LEFT, <<)                                                       \
  ACTION(SHIFT_RIGHT, >>)                                                      \
  ACTION(SHIFT_RIGHT_UNSIGNED, >>)                                             \
  ACTION(LOG_BASE, )                                                           \
  ACTION(ATAN2, )                                                              \
  ACTION(PMOD, %)                                                              \
  ACTION(NULL_EQUALS, ==)                                                      \
  ACTION(NULL_MAX, )                                                           \
  ACTION(NULL_MIN, )

#define DEF_BINARY_OPERATOR_ENUM(OP, PRETTY) OP,

enum class BinaryOperator : int32_t {
  APPLY_FOR_BINARY_OPERATORS(DEF_BINARY_OPERATOR_ENUM)
};

#undef DEF_BINARY_OPERATOR_ENUM

inline std::string binaryOperatorToString(BinaryOperator op) {
#define BINARY_OP_CASE(OP, PRETTY)                                             \
  case BinaryOperator::OP:                                                     \
    return CURA_STRINGIFY(OP);

  switch (op) {
    APPLY_FOR_BINARY_OPERATORS(BINARY_OP_CASE);
  default:
    CURA_FAIL("Unknown binary op " + std::to_string(static_cast<int32_t>(op)));
  }

#undef BINARY_OP_CASE
}

inline std::string binaryOperatorPretty(BinaryOperator op) {
#define BINARY_OP_CASE(OP, PRETTY)                                             \
  case BinaryOperator::OP:                                                     \
    return CURA_STRINGIFY(PRETTY);

  switch (op) {
    APPLY_FOR_BINARY_OPERATORS(BINARY_OP_CASE);
  default:
    CURA_FAIL("Unknown binary op " + std::to_string(static_cast<int32_t>(op)));
  }

#undef BINARY_OP_CASE
}

inline BinaryOperator binaryOperatorFromString(const std::string &s) {
#define BINARY_OP_CASE(OP, PRETTY)                                             \
  if (s == CURA_STRINGIFY(OP)) {                                               \
    return BinaryOperator::OP;                                                 \
  }

  APPLY_FOR_BINARY_OPERATORS(BINARY_OP_CASE)

  CURA_FAIL("Invalid binary operator: " + s);

#undef BINARY_OP_CASE
}

struct BinaryOp : public Op {
  BinaryOp(BinaryOperator binary_operator_,
           std::shared_ptr<const Expression> left,
           std::shared_ptr<const Expression> right, DataType data_type)
      : Op({left, right}, data_type), binary_operator(binary_operator_) {
    CURA_ASSERT(left, "Invalid left operand for BinaryOp");
    CURA_ASSERT(right, "Invalid right operand for BinaryOp");
  }

  const DataType &dataType() const override { return data_type; }

  std::string name() const override { return "BinaryOp"; }

  std::string toString() const override {
    auto pretty = binaryOperatorPretty(binary_operator);
    if (pretty.empty()) {
      return binaryOperatorToString(binary_operator) + "(" +
             operands_[0]->toString() + ", " + operands_[1]->toString() + ")";
    }
    return operands_[0]->toString() + " " + pretty + " " +
           operands_[1]->toString();
  }

  BinaryOperator binaryOperator() const { return binary_operator; }

  std::shared_ptr<const Expression> left() const { return operands_[0]; }

  std::shared_ptr<const Expression> right() const { return operands_[1]; }

private:
  BinaryOperator binary_operator;
};

} // namespace cura::expression
