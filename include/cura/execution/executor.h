#pragma once

#include "cura/execution/pipeline.h"

#include <list>

namespace cura::driver {
struct Option;
} // namespace cura::driver

namespace cura::execution {

using cura::driver::Option;

struct Executor {
public:
  explicit Executor(const Option &option_,
                    std::list<std::unique_ptr<Pipeline>> &&pipelines_)
      : option(option_), pipelines(std::move(pipelines_)), ctx(option) {}

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
  Pipeline &currentPipeline() const;

private:
  const Option &option;
  Context ctx;
  std::list<std::unique_ptr<Pipeline>> pipelines;
};

} // namespace cura::execution
