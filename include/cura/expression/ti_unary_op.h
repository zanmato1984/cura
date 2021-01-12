#pragma once

#include "cura/expression/expressions.h"

namespace cura::expression {

#define APPLY_FOR_TI_UNARY_OPERATORS(ACTION) ACTION(EXTRACT_YEAR, extract_year)

struct TiUnaryOp : public BaseUnaryOp {
#define DEF_UNARY_OPERATOR_ENUM(OP, PRETTY) OP,

  enum class UnaryOperator : int32_t {
    APPLY_FOR_TI_UNARY_OPERATORS(DEF_UNARY_OPERATOR_ENUM)
  };

#undef DEF_TI_UNARY_OPERATOR_ENUM

  static std::string unaryOperatorToString(UnaryOperator op);
  static std::string unaryOperatorPretty(UnaryOperator op);
  static UnaryOperator unaryOperatorFromString(const std::string &s);

  TiUnaryOp(std::shared_ptr<const Expression> operand, DataType data_type,
            UnaryOperator unary_operator_)
      : BaseUnaryOp(operand, std::move(data_type)),
        unary_operator(unary_operator_) {}

  std::string name() const override { return "TiUnaryOp"; }

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
