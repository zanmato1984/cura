#pragma once

#include <stdexcept>

namespace cura {

struct LogicalError : public std::logic_error {
  explicit LogicalError(const char *message) : std::logic_error(message) {}

  explicit LogicalError(const std::string &message)
      : std::logic_error(message) {}

  // TODO Add an error code member? This would be useful for translating an
  // exception to an error code in a pure-C API
};

} // namespace cura

#define STRINGIFY_DETAIL(x) #x
#define CURA_STRINGIFY(x) STRINGIFY_DETAIL(x)

#define CURA_FAIL(reason)                                                      \
  throw cura::LogicalError("CURA failure at: " __FILE__                        \
                           ":" CURA_STRINGIFY(__LINE__) ": " reason)

#define CURA_ASSERT(cond, reason)                                              \
  (!!(cond)) ? static_cast<void>(0) : CURA_FAIL(reason)

#define CURA_ASSERT_JSON(json)                                                 \
  CURA_ASSERT(json, CURA_STRINGIFY(json) " is false");

#define CURA_ASSERT_ARROW_OK(arrow, reason)                                    \
  {                                                                            \
    const auto &status = (arrow);                                              \
    CURA_ASSERT(status.ok(), reason ": " + status.ToString());                 \
  }

#define CURA_GET_ARROW_RESULT(arrow)                                           \
  [&]() {                                                                      \
    auto &&res = (arrow);                                                      \
    CURA_ASSERT_ARROW_OK(res.status(), "Arrow get result failed");             \
    return std::move(res.ValueUnsafe());                                       \
  }()
