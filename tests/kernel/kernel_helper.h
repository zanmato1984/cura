#pragma once

#include "cura/driver/option.h"
#include "cura/execution/context.h"

using cura::driver::Option;
using cura::execution::Context;

inline Context makeTrivialContext(const Option &option) {
  return Context(option);
}
