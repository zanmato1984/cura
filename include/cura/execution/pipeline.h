#pragma once

#include "cura/common/types.h"
#include "cura/execution/context.h"

#include <memory>
#include <unordered_map>
#include <vector>

namespace cura::data {
struct Fragment;
} // namespace cura::data

namespace cura::kernel {
struct Source;
struct Terminal;
} // namespace cura::kernel

namespace cura::execution {

using cura::data::Fragment;
using cura::kernel::Source;
using cura::kernel::Terminal;

class Pipeline {
public:
  /// Pipeline static attributes.
  const PipelineId id;
  const bool is_final;
  std::vector<SourceId> source_ids;
  std::unordered_map<SourceId, std::shared_ptr<const Source>> sources;
  const std::shared_ptr<Terminal> terminal;

private:
  /// Pipeline runtime states.
  size_t current_source;

public:
  Pipeline(PipelineId id_, bool is_final_,
           const std::vector<std::shared_ptr<const Source>> &sources_,
           std::shared_ptr<Terminal> terminal_ = nullptr);

public:
  bool hasNextSource() const;

  SourceId nextSource();

  /// Parallel-able.
  void push(const Context &ctx, ThreadId thread_id, SourceId source_id,
            std::shared_ptr<const Fragment> fragment);

  /// Parallel-able.
  std::shared_ptr<const Fragment>
  stream(const Context &ctx, ThreadId thread_id, SourceId source_id,
         std::shared_ptr<const Fragment> fragment, size_t rows) const;

  std::string toString() const;

private:
  std::shared_ptr<const Source> getSource(SourceId source_id) const;

  friend class Executor;
};

} // namespace cura::execution
