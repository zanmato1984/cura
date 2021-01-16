#pragma once

#include "cura/data/fragment.h"
#include "cura/execution/memory_resource.h"

namespace cura::kernel::detail {

using cura::data::Fragment;
using cura::execution::MemoryResource;
using cura::type::Schema;

std::shared_ptr<Fragment>
concatFragments(MemoryResource::Underlying *underlying, const Schema &schema,
                const std::vector<std::shared_ptr<const Fragment>> &fragments);

using Key = std::vector<std::shared_ptr<arrow::Scalar>>;

struct RowHash {
  size_t operator()(const Key &key) const {
    return std::accumulate(
        key.begin(), key.end(), 0, [](size_t hash, const auto &col) {
          return hash ^= col->is_valid ? arrow::Scalar::Hash::hash(*col) : 0;
        });
  }
};

struct RowEqual {
  bool operator()(const Key &l, const Key &r) const {
    if (l.size() != r.size()) {
      return false;
    }
    for (size_t i = 0; i < l.size(); i++) {
      if (!scalar_equal(l[i], r[i])) {
        return false;
      }
    }
    return true;
  }

  arrow::Scalar::PtrsEqual scalar_equal;
};

} // namespace cura::kernel::detail
