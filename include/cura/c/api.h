#ifndef API_H
#define API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowSchema;
struct ArrowArray;

typedef void *DriverHandle;
typedef int64_t ThreadId;
typedef int64_t SourceId;

DriverHandle create_driver();
void release_driver(DriverHandle handle);

int explain(DriverHandle handle, const char *json, int extended,
            char ***result);
int compile(DriverHandle handle, const char *json);

int set_memory_resource(DriverHandle handle, int8_t memory_resource);
int set_exclusive_default_memory_resource(DriverHandle handle,
                                          int8_t exclusive);
int set_memory_resource_size(DriverHandle handle, size_t size);
int set_threads_per_pipeline(DriverHandle handle, size_t threads_per_pipeline);
int set_memory_resource_size_per_thread(DriverHandle handle, size_t size);
int set_bucket_aggregate(DriverHandle handle, int8_t enable);
int set_bucket_aggregate_buckets(DriverHandle handle, size_t buckets);

int has_next_pipeline(DriverHandle handle);
int prepare_pipeline(DriverHandle handle);
int is_pipeline_final(DriverHandle handle);
int finish_pipeline(DriverHandle handle);

int pipeline_has_next_source(DriverHandle handle);
SourceId pipeline_next_source(DriverHandle handle);
int is_heap_source(SourceId source_id);

int pipeline_push(DriverHandle handle, ThreadId thread_id, SourceId source_id,
                  struct ArrowArray *input, struct ArrowSchema *schemas);
int pipeline_stream(DriverHandle handle, ThreadId thread_id, SourceId source_id,
                    struct ArrowArray *input, struct ArrowSchema *input_schema,
                    struct ArrowArray *output,
                    struct ArrowSchema *output_schema, size_t rows);

#ifdef __cplusplus
}
#endif

#endif