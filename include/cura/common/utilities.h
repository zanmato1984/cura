#pragma once

#include "cura/common/errors.h"
#include "cura/common/types.h"

namespace cura {

template <typename ID> inline SourceId makeHeapSourceId(ID id) {
  CURA_ASSERT(id > 0,
              "Making heap source ID from a non-positive ID is not allowed");
  return -static_cast<SourceId>(id);
}

inline bool isHeapSourceId(SourceId source_id) { return source_id < 0; }

} // namespace cura
