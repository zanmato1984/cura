#include "cura/c/api.h"
#include "cura/common/errors.h"
#include "cura/common/utilities.h"
#include "cura/data/fragment.h"
#include "cura/driver/driver.h"

#include <arrow/c/bridge.h>
#include <iostream>
#include <string>

using cura::isHeapSourceId;
using cura::data::Fragment;
using cura::driver::Driver;
using ArrowArray = struct ArrowArray;
using ArrowSchema = struct ArrowSchema;

inline Driver *getDriver(DriverHandle handle) {
  return static_cast<Driver *>(handle);
}

std::shared_ptr<Fragment> importArrowFragment(ArrowArray *input,
                                              ArrowSchema *schema) {
  if (!input) {
    return nullptr;
  }

  auto rb = CURA_GET_ARROW_RESULT(arrow::ImportRecordBatch(input, schema));
  return std::make_shared<Fragment>(rb);
}

size_t exportArrowFragment(std::shared_ptr<const Fragment> fragment,
                           ArrowArray *out, ArrowSchema *schema) {
  if (!fragment) {
    return 0;
  }

  auto rb = fragment->arrow();
  CURA_ASSERT_ARROW_OK(arrow::ExportRecordBatch(*rb, out, schema),
                       "Export record batch failed");
  return 1;
}

#ifdef __cplusplus
extern "C" {
#endif

DriverHandle create_driver() { return new Driver; }

void release_driver(DriverHandle handle) { delete getDriver(handle); }

int explain(DriverHandle handle, const char *json, int extended,
            char ***result) {
  try {
    auto explained = getDriver(handle)->explain(std::string(json), extended);
    int n = explained.size();
    *result = static_cast<char **>(malloc(sizeof(char *) * n));
    for (int i = 0; i < n; i++) {
      (*result)[i] = static_cast<char *>(malloc(explained[i].length() + 1));
      memcpy((*result)[i], explained[i].data(), explained[i].length() + 1);
    }
    return n;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int compile(DriverHandle handle, const char *json) {
  try {
    getDriver(handle)->compile(std::string(json));
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int set_memory_resource(DriverHandle handle, int8_t memory_resource) {
  try {
    getDriver(handle)->setMemoryResource(memory_resource);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int set_exclusive_default_memory_resource(DriverHandle handle,
                                          int8_t exclusive) {
  try {
    getDriver(handle)->setExclusiveDefaultMemoryResource(exclusive);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int set_memory_resource_size(DriverHandle handle, size_t size) {
  try {
    getDriver(handle)->setMemoryResourceSize(size);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int set_threads_per_pipeline(DriverHandle handle, size_t threads_per_pipeline) {
  try {
    getDriver(handle)->setThreadsPerPipeline(threads_per_pipeline);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int set_memory_resource_size_per_thread(DriverHandle handle, size_t size) {
  try {
    getDriver(handle)->setMemoryResourceSizePerThread(size);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int set_bucket_aggregate(DriverHandle handle, int8_t enable) {
  try {
    getDriver(handle)->setBucketAggregate(enable);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int set_bucket_aggregate_buckets(DriverHandle handle, size_t buckets) {
  try {
    getDriver(handle)->setBucketAggregateBuckets(buckets);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int has_next_pipeline(DriverHandle handle) {
  return getDriver(handle)->hasNextPipeline();
}

int prepare_pipeline(DriverHandle handle) {
  try {
    getDriver(handle)->preparePipeline();
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int is_pipeline_final(DriverHandle handle) {
  return getDriver(handle)->isPipelineFinal();
}

int finish_pipeline(DriverHandle handle) {
  try {
    getDriver(handle)->finishPipeline();
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int pipeline_has_next_source(DriverHandle handle) {
  return getDriver(handle)->pipelineHasNextSource();
}

SourceId pipeline_next_source(DriverHandle handle) {
  return static_cast<SourceId>(getDriver(handle)->pipelineNextSource());
}

int is_heap_source(SourceId source_id) { return isHeapSourceId(source_id); }

int pipeline_push(DriverHandle handle, ThreadId thread_id, SourceId source_id,
                  ArrowArray *input, ArrowSchema *schema) {
  try {
    auto fragment = importArrowFragment(input, schema);
    getDriver(handle)->pipelinePush(thread_id, source_id, fragment);
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

int pipeline_stream(DriverHandle handle, ThreadId thread_id, SourceId source_id,
                    ArrowArray *input, ArrowSchema *input_schema,
                    ArrowArray *output, ArrowSchema *output_schema,
                    size_t rows) {
  try {
    auto input_fragment = importArrowFragment(input, input_schema);
    auto output_fragment = getDriver(handle)->pipelineStream(
        thread_id, source_id, input_fragment, rows);
    return exportArrowFragment(output_fragment, output, output_schema);
  } catch (const std::exception &e) {
    std::cerr << e.what() << std::endl;
    return -1;
  } catch (...) {
    return -1;
  }
}

#ifdef __cplusplus
}
#endif
