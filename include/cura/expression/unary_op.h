#pragma once

#include "cura/expression/expressions.h"

namespace cura::expression {

#define APPLY_FOR_UNARY_OPERATORS(ACTION)                                      \
  ACTION(SIN, sin)                                                             \
  ACTION(COS, cos)                                                             \
  ACTION(TAN, tan)                                                             \
  ACTION(ARCSIN, atan)                                                         \
  ACTION(ARCCOS, acos)                                                         \
  ACTION(ARCTAN, atan)                                                         \
  ACTION(SINH, sinh)                                                           \
  ACTION(COSH, cosh)                                                           \
  ACTION(TANH, tanh)                                                           \
  ACTION(ARCSINH, asinh)                                                       \
  ACTION(ARCCOSH, acosh)                                                       \
  ACTION(ARCTANH, atanh)                                                       \
  ACTION(EXP, exp)                                                             \
  ACTION(LOG, log)                                                             \
  ACTION(SQRT, sqrt)                                                           \
  ACTION(CBRT, cbrt)                                                           \
  ACTION(CEIL, cel)                                                            \
  ACTION(FLOOR, floor)                                                         \
  ACTION(ABS, abs)                                                             \
  ACTION(RINT, rint)                                                           \
  ACTION(BIT_INVERT, ~)                                                        \
  ACTION(NOT, !)

struct UnaryOp : public BaseUnaryOp {
#define DEF_UNARY_OPERATOR_ENUM(OP, PRETTY) OP,

  enum class UnaryOperator : int32_t {
    APPLY_FOR_UNARY_OPERATORS(DEF_UNARY_OPERATOR_ENUM)
  };

#undef DEF_UNARY_OPERATOR_ENUM

  static std::string unaryOperatorToString(UnaryOperator op);
  static std::string unaryOperatorPretty(UnaryOperator op);
  static UnaryOperator unaryOperatorFromString(const std::string &s);

  UnaryOp(std::shared_ptr<const Expression> operand, DataType data_type,
          UnaryOperator unary_operator_)
      : BaseUnaryOp(operand, std::move(data_type)),
        unary_operator(unary_operator_) {}

  std::string name() const override { return "UnaryOp"; }

  std::string operatorToString() const override {
    auto pretty = unaryOperatorPretty(unary_operator);
    if (pretty.empty()) {
      return unaryOperatorToString(unary_operator);
    }
    return pretty;
  }

  UnaryOperator unaryOperator() const { return unary_operator; }

private:
  UnaryOperator unary_operator;
};

} // namespace cura::expression
