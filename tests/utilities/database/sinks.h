#pragma once

#include "cura/data/fragment.h"

#include <functional>

namespace cura::test::database {

using cura::data::Fragment;

struct PrintArrowArrayPerLineSink {
  static void sink(const Fragment &fragment) {
    std::stringstream ss;

    ss << "{" << std::endl;
    auto arrow = fragment.arrow();
    for (size_t i = 0; i < arrow->num_columns(); i++) {
      auto column = arrow->column(i);
      auto s = column->ToString();
      s.erase(std::remove(s.begin(), s.end(), '\n'), s.end());
      s.erase(std::remove(s.begin(), s.end(), ' '), s.end());
      ss << s << std::endl;
    }
    ss << "}" << std::endl;

    std::cout << ss.str();
  }
};

} // namespace cura::test::database
