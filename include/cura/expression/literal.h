#pragma once

#include "cura/expression/expressions.h"

#include <arrow/api.h>
#include <arrow/visitor_inline.h>

namespace cura::expression {

using cura::type::TypeId;

struct Literal : public Expression {
  const DataType &dataType() const override { return data_type; }

  std::string name() const override { return "Literal"; }

  std::string toString() const override;

  template <typename T> T value() const {
    CURA_FAIL("Unimplemented");
  }

private:
  DataType data_type;
};

} // namespace cura::expression
