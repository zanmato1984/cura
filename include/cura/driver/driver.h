#pragma once

#include "cura/common/types.h"
#include "cura/driver/option.h"
#include "cura/execution/context.h"
#include "cura/execution/executor.h"

#include <list>

namespace cura::data {
struct Fragment;
} // namespace cura::data

namespace cura::relational {
struct Rel;
} // namespace cura::relational

namespace cura::driver {

using cura::data::Fragment;
using cura::execution::Context;
using cura::execution::Executor;
using cura::relational::Rel;

/// Entry of CURA CPP API.
/// Users can compose their own CURA plans using classes under namespace
/// `cura::relational`, or assemble plans into JSON format string like CURA C
/// API does, and pass them into the corresponding `compile` methods. Data
/// passed around is represented using arrow class `arrow::Array` and the life
/// cycle is fully managed using CPP shared pointer.
class Driver {
public:
  /// Explain APIs.
  std::vector<std::string> explain(const std::string &json, bool extended);
  std::vector<std::string> explain(std::shared_ptr<const Rel> rel,
                                   bool extended);

  /// Compile APIs.
  void compile(const std::string &json);
  void compile(std::shared_ptr<const Rel> rel);

  /// Setting APIs.
  void setMemoryResource(int8_t memory_resouce);
  void setExclusiveDefaultMemoryResource(bool exclusive);
  void setMemoryResourceSize(size_t size);
  void setThreadsPerPipeline(size_t threads);
  void setMemoryResourceSizePerThread(size_t size);
  void setBucketAggregate(bool enable);
  void setBucketAggregateBuckets(size_t buckets);

  /// Execution APIs.
  bool hasNextPipeline() const;
  void preparePipeline();
  bool isPipelineFinal() const;
  void finishPipeline();

  bool pipelineHasNextSource() const;
  SourceId pipelineNextSource();

  void pipelinePush(ThreadId thread_id, SourceId source_id,
                    std::shared_ptr<const Fragment> fragment);
  std::shared_ptr<const Fragment>
  pipelineStream(ThreadId thread_id, SourceId source_id,
                 std::shared_ptr<const Fragment> fragment, size_t rows) const;

private:
  Option option;
  std::unique_ptr<Executor> executor;
};

} // namespace cura::driver
