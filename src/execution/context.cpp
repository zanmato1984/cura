#include "cura/execution/context.h"

namespace cura::execution {

Context::Context(const Option &option_)
    : option(option_), memory_resource(createMemoryResource(option)) {}

} // namespace cura::execution