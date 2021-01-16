#include "cura/expression/unary_op.h"

namespace cura::expression {

std::string UnaryOp::unaryOperatorToString(UnaryOperator op) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  case UnaryOperator::OP:                                                      \
    return CURA_STRINGIFY(OP);

  switch (op) {
    APPLY_FOR_UNARY_OPERATORS(UNARY_OP_CASE);
  default:
    CURA_FAIL("Unknown unary op " + std::to_string(static_cast<int32_t>(op)));
  }

#undef UNARY_OP_CASE
}

std::string UnaryOp::unaryOperatorPretty(UnaryOperator op) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  case UnaryOperator::OP:                                                      \
    return CURA_STRINGIFY(PRETTY);

  switch (op) {
    APPLY_FOR_UNARY_OPERATORS(UNARY_OP_CASE);
  default:
    CURA_FAIL("Unknown unary op " + std::to_string(static_cast<int32_t>(op)));
  }

#undef UNARY_OP_CASE
}

UnaryOp::UnaryOperator UnaryOp::unaryOperatorFromString(const std::string &s) {
#define UNARY_OP_CASE(OP, PRETTY)                                              \
  if (s == CURA_STRINGIFY(OP)) {                                               \
    return UnaryOperator::OP;                                                  \
  }

  APPLY_FOR_UNARY_OPERATORS(UNARY_OP_CASE)

  CURA_FAIL("Invalid unary operator: " + s);

#undef UNARY_OP_CASE
}

std::shared_ptr<const Column>
UnaryOp::evaluate(const Context &ctx, ThreadId thread_id,
                  const Fragment &fragment) const {
  // TODO: Implement.
  CURA_FAIL("Not implemented");
}

} // namespace cura::expression