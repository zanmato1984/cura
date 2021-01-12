#pragma once

#include "cura/common/errors.h"
#include "cura/expression/literal.h"

#include <memory>

namespace cura::expression {

template <typename Impl, typename Result> struct ExpressionVisitor {
  Result visit(std::shared_ptr<const Expression> expression) {
    if (auto literal = std::dynamic_pointer_cast<const Literal>(expression);
        literal) {
      return impl().visitLiteral(literal);
    }

    if (auto column_ref =
            std::dynamic_pointer_cast<const ColumnRef>(expression);
        column_ref) {
      return impl().visitColumnRef(column_ref);
    }

    if (auto op = std::dynamic_pointer_cast<const Op>(expression); op) {
      std::vector<Result> operands;
      for (const auto &operand : op->operands()) {
        operands.emplace_back(visit(operand));
      }
      return impl().visitOp(op, operands);
    }

    CURA_FAIL("Unknown Expression type.");
  }

  Result visitLiteral(std::shared_ptr<const Literal> literal) {
    return impl().defaultVisit(literal, {});
  }

  Result visitColumnRef(std::shared_ptr<const ColumnRef> column_ref) {
    return impl().defaultVisit(column_ref, {});
  }

  Result visitOp(std::shared_ptr<const Op> op,
                 const std::vector<Result> &children) {
    return impl().defaultVisit(op, children);
  }

  Result defaultVisit(std::shared_ptr<const Expression>,
                      const std::vector<Result> &children) {
    return {};
  }

private:
  ExpressionVisitor() = default;
  friend Impl;

  Impl &impl() { return *static_cast<Impl *>(this); }

  const Impl &impl() const { return *static_cast<const Impl *>(this); }
};

template <typename Impl> struct ExpressionVisitor<Impl, void> {
  void visit(std::shared_ptr<const Expression> expression) {
    if (auto literal = std::dynamic_pointer_cast<const Literal>(expression);
        literal) {
      impl().visitLiteral(literal);
      return;
    }

    if (auto column_ref =
            std::dynamic_pointer_cast<const ColumnRef>(expression);
        column_ref) {
      impl().visitColumnRef(column_ref);
      return;
    }

    if (auto op = std::dynamic_pointer_cast<const Op>(expression); op) {
      for (const auto &operand : op->operands()) {
        visit(operand);
      }
      impl().visitOp(op);
      return;
    }

    CURA_FAIL("Unknown Expression type.");
  }

  void visitLiteral(std::shared_ptr<const Literal> literal) {
    impl().defaultVisit(literal);
  }

  void visitColumnRef(std::shared_ptr<const ColumnRef> column_ref) {
    impl().defaultVisit(column_ref);
  }

  void visitOp(std::shared_ptr<const Op> op) { impl().defaultVisit(op); }

  void defaultVisit(std::shared_ptr<const Expression>) {}

private:
  ExpressionVisitor() = default;
  friend Impl;

  Impl &impl() { return *static_cast<Impl *>(this); }

  const Impl &impl() const { return *static_cast<const Impl *>(this); }
};

} // namespace cura::expression