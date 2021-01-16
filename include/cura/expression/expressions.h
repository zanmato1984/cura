#pragma once

#include "cura/common/errors.h"
#include "cura/data/column.h"
#include "cura/execution/context.h"
#include "cura/type/data_type.h"

#include <memory>
#include <vector>

namespace cura::data {
struct Fragment;
} // namespace cura::data

namespace cura::expression {

using cura::data::Column;
using cura::data::Fragment;
using cura::execution::Context;
using cura::type::DataType;

struct Expression {
  virtual ~Expression() = default;

  virtual const DataType &dataType() const = 0;

  virtual std::shared_ptr<const Column>
  evaluate(const Context &ctx, ThreadId thread_id,
           const Fragment &fragment) const = 0;

  virtual std::string name() const = 0;

  virtual std::string toString() const { return name(); }
};

using ColumnIdx = size_t;

struct ColumnRef : public Expression {
  ColumnRef(ColumnIdx idx_, DataType data_type_)
      : idx(idx_), data_type(std::move(data_type_)) {}

  const DataType &dataType() const override { return data_type; }

  std::shared_ptr<const Column>
  evaluate(const Context &ctx, ThreadId thread_id,
           const Fragment &fragment) const override;

  std::string name() const override { return "Column"; }

  std::string toString() const override {
    return Expression::toString() + "#" + std::to_string(idx);
  }

  ColumnIdx columnIdx() const { return idx; }

private:
  ColumnIdx idx;
  DataType data_type;
};

struct Op : public Expression {
  Op(std::vector<std::shared_ptr<const Expression>> operands,
     DataType data_type_)
      : operands_(std::move(operands)), data_type(std::move(data_type_)) {}

  const DataType &dataType() const override { return data_type; }

  const std::vector<std::shared_ptr<const Expression>> &operands() const {
    return operands_;
  }

protected:
  std::vector<std::shared_ptr<const Expression>> operands_;
  DataType data_type;
};

struct BaseUnaryOp : public Op {
  BaseUnaryOp(std::shared_ptr<const Expression> operand, DataType data_type)
      : Op({operand}, data_type) {
    CURA_ASSERT(operand, "Invalid operand for BaseUnaryOp");
  }

  const DataType &dataType() const override { return data_type; }

  std::string toString() const override {
    return operatorToString() + "(" + operands_[0]->toString() + ")";
  }

  virtual std::string operatorToString() const = 0;
};

} // namespace cura::expression