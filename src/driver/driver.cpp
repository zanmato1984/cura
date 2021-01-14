#include "cura/driver/driver.h"
#include "cura/common/errors.h"
#include "cura/common/utilities.h"
#include "cura/planning/planner.h"
#include "cura/relational/parsers.h"

namespace cura::driver {

using cura::planning::Planner;
using cura::relational::parseJson;

std::vector<std::string> Driver::explain(const std::string &json,
                                         bool extended) {
  auto rel = parseJson(json);
  return explain(rel, extended);
}

std::vector<std::string> Driver::explain(std::shared_ptr<const Rel> rel,
                                         bool extended) {
  return Planner(option).explain(rel, extended);
}

void Driver::compile(const std::string &json) {
  auto rel = parseJson(json);
  compile(rel);
}

void Driver::compile(std::shared_ptr<const Rel> rel) {
  auto pipelines = Planner(option).plan(rel);
  executor = std::make_unique<Executor>(option, std::move(pipelines));
}

void Driver::setMemoryResource(int8_t memory_resouce) {
  option.memory_resource = memory_resouce;
}

void Driver::setExclusiveDefaultMemoryResource(bool exclusive) {
  option.exclusive_default_memory_resource = exclusive;
}

void Driver::setMemoryResourceSize(size_t size) {
  option.memory_resource_size = size;
}

void Driver::setThreadsPerPipeline(size_t threads) {
  CURA_ASSERT(threads, "Threads per pipeline couldn't be 0");

  option.threads_per_pipeline = threads;
}

void Driver::setMemoryResourceSizePerThread(size_t size) {
  CURA_ASSERT(size, "Memory resource size per thread couldn't be 0");

  option.memory_resource_size_per_thread = size;
}

void Driver::setBucketAggregate(bool enable) {
  option.bucket_aggregate = enable;
}

void Driver::setBucketAggregateBuckets(size_t buckets) {
  CURA_ASSERT(buckets, "Buckets of bucket aggregate couldn't be 0");

  option.bucket_aggregate_buckets = buckets;
}

bool Driver::hasNextPipeline() const {
  CURA_ASSERT(executor, "No plan compiled in driver");

  return executor->hasNextPipeline();
}

void Driver::preparePipeline() {
  CURA_ASSERT(executor, "No plan compiled in driver");

  executor->preparePipeline();
}

bool Driver::isPipelineFinal() const {
  CURA_ASSERT(executor, "No plan compiled in driver");

  return executor->isPipelineFinal();
}

void Driver::finishPipeline() {
  CURA_ASSERT(executor, "No plan compiled in driver");

  executor->finishPipeline();
}

bool Driver::pipelineHasNextSource() const {
  CURA_ASSERT(executor, "No plan compiled in driver");

  return executor->pipelineHasNextSource();
}

SourceId Driver::pipelineNextSource() {
  CURA_ASSERT(executor, "No plan compiled in driver");

  return executor->pipelineNextSource();
}

void Driver::pipelinePush(ThreadId thread_id, SourceId source_id,
                          std::shared_ptr<const Fragment> fragment) {
  CURA_ASSERT(executor, "No plan compiled in driver");

  executor->pipelinePush(thread_id, source_id, fragment);
}

std::shared_ptr<const Fragment>
Driver::pipelineStream(ThreadId thread_id, SourceId source_id,
                       std::shared_ptr<const Fragment> fragment,
                       size_t rows) const {
  CURA_ASSERT(executor, "No plan compiled in driver");

  return executor->pipelineStream(thread_id, source_id, fragment, rows);
}

} // namespace cura::driver
