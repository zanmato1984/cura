#include "cura/execution/executor.h"
#include "cura/common/errors.h"
#include "cura/kernel/kernel.h"

namespace cura::execution {

bool Executor::hasNextPipeline() const { return !pipelines.empty(); }

void Executor::preparePipeline() {
  ctx.memory_resource->allocatePreConcatenate();
}

bool Executor::isPipelineFinal() const { return currentPipeline().is_final; }

void Executor::finishPipeline() {
  if (!isPipelineFinal()) {
    /// Release sources that holds buffers never used anymore.
    currentPipeline().sources.clear();
    ctx.memory_resource->reclaimConverge();

    /// Start concatenating and converging non-stream kernels in pipeline.
    auto non_stream_kernels =
        std::move(currentPipeline().terminal->non_stream_kernels);
    for (const auto &kernel : non_stream_kernels) {
      kernel->concatenate(ctx);
      kernel->converge(ctx);
    }
  }

  pipelines.pop_front();
}

bool Executor::pipelineHasNextSource() const {
  return currentPipeline().hasNextSource();
}

SourceId Executor::pipelineNextSource() {
  return currentPipeline().nextSource();
}

void Executor::pipelinePush(ThreadId thread_id, SourceId source_id,
                            std::shared_ptr<const Fragment> fragment) {
  currentPipeline().push(ctx, thread_id, source_id, fragment);
}

std::shared_ptr<const Fragment>
Executor::pipelineStream(ThreadId thread_id, SourceId source_id,
                         std::shared_ptr<const Fragment> fragment,
                         size_t rows) const {
  return currentPipeline().stream(ctx, thread_id, source_id, fragment, rows);
}

Pipeline &Executor::currentPipeline() const {
  CURA_ASSERT(hasNextPipeline(), "No pipeline left in executor");
  return *pipelines.front();
}

} // namespace cura::execution
