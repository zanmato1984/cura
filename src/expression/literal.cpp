#include "cura/expression/literal.h"
#include "cura/data/column_factories.h"
#include "cura/data/column_scalar.h"
#include "cura/data/fragment.h"

namespace cura::expression {

#ifdef USE_CUDF
using cura::data::createCudfColumnScalar;
#else
using cura::data::createArrowColumnScalar;
#endif

#ifdef USE_CUDF
namespace detail {

struct ToString {
  template <typename T, std::enable_if_t<cudf::is_boolean<T>()> * = nullptr>
  std::string operator()(const cudf::scalar &scalar) const {
    auto p = static_cast<const cudf::numeric_scalar<T> *>(&scalar);
    return p->value() ? "true" : "false";
  }

  template <typename T, std::enable_if_t<cudf::is_numeric<T>() &&
                                         !cudf::is_boolean<T>()> * = nullptr>
  std::string operator()(const cudf::scalar &scalar) const {
    auto p = static_cast<const cudf::numeric_scalar<T> *>(&scalar);
    return std::to_string(p->value());
  }

  template <typename T, std::enable_if_t<cudf::is_timestamp<T>()> * = nullptr>
  std::string operator()(const cudf::scalar &scalar) const {
    auto p = const_cast<cudf::timestamp_scalar<T> *>(
        static_cast<const cudf::timestamp_scalar<T> *>(&scalar));
    return std::to_string(p->ticks_since_epoch());
  }

  template <typename T, std::enable_if_t<cudf::is_duration<T>()> * = nullptr>
  std::string operator()(const cudf::scalar &scalar) const {
    auto p = const_cast<cudf::duration_scalar<T> *>(
        static_cast<const cudf::duration_scalar<T> *>(&scalar));
    return std::to_string(p->count());
  }

  template <typename T, std::enable_if_t<!cudf::is_numeric<T>() &&
                                         !cudf::is_chrono<T>()> * = nullptr>
  std::string operator()(const cudf::scalar &scalar) const {
    auto p = static_cast<const cudf::string_scalar *>(&scalar);
    return p->to_string();
  }
};

} // namespace detail
#endif

std::shared_ptr<const Column>
Literal::evaluate(const Context &ctx, ThreadId thread_id,
                  const Fragment &fragment) const {
#ifdef USE_CUDF
  return createCudfColumnScalar(data_type, fragment.size(), scalar);
#else
  return createArrowColumnScalar(data_type, fragment.size(), scalar);
#endif
}

std::string Literal::toString() const {
#ifdef USE_CUDF
  return cudf::type_dispatcher(scalar->type(), detail::ToString{}, *scalar);
#else
  return scalar->ToString();
#endif
}

} // namespace cura::expression
