package cura

// #include "c/arrow.h"
//
// #include <cura/c/api.h>
// #include <stdlib.h>
import (
	"C"
)

import (
	"github.com/apache/arrow/go/arrow/array"
	"unsafe"
)

type MemoryResource int8

const (
	Arena MemoryResource = iota
	ArenaPerThread
	Pool
	PoolPerThread
	Managed
	Cuda
)

type Driver struct {
	handle C.DriverHandle
}

func CreateDriver() *Driver {
	driver := &Driver{
		handle: C.create_driver(),
	}

	return driver
}

func ReleaseDriver(d *Driver) {
	C.release_driver(d.handle)
}

func (d *Driver) Explain(json string, extended bool) (int, []string) {
	cs := C.CString(json)
	defer C.free(unsafe.Pointer(cs))
	var ci = 0
	if extended {
		ci = 1
	}
	var out **C.char
	n := int(C.explain(d.handle, cs, C.int(ci), &out))
	if n <= 0 {
		return n, make([]string, 0)
	}
	outSlice := (*[1 << 28]*C.char)(unsafe.Pointer(out))[:n:n]
	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = C.GoString(outSlice[i])
		C.free(unsafe.Pointer(outSlice[i]))
	}
	C.free(unsafe.Pointer(out))
	return n, result
}

func (d *Driver) Compile(json string) int {
	cs := C.CString(json)
	defer C.free(unsafe.Pointer(cs))
	return int(C.compile(d.handle, cs))
}

func (d *Driver) SetMemoryResource(memoryResource MemoryResource) int {
	return int(C.set_memory_resource(d.handle, C.int8_t(memoryResource)))
}

func (d *Driver) SetExclusiveDefaultMemoryResource(exclusive bool) int {
	if exclusive {
		return int(C.set_exclusive_default_memory_resource(d.handle, C.int8_t(1)))
	}
	return int(C.set_exclusive_default_memory_resource(d.handle, C.int8_t(0)))
}

func (d *Driver) SetMemoryResourceSize(size uint64) int {
	return int(C.set_memory_resource_size(d.handle, C.size_t(size)))
}

func (d *Driver) SetThreadsPerPipeline(threadsPerPipeline uint64) int {
	return int(C.set_threads_per_pipeline(d.handle, C.size_t(threadsPerPipeline)))
}

func (d *Driver) SetMemoryResourceSizePerThread(size uint64) int {
	return int(C.set_memory_resource_size_per_thread(d.handle, C.size_t(size)))
}

func (d *Driver) SetBucketAggregate(enable bool) int {
	if enable {
		return int(C.set_bucket_aggregate(d.handle, C.int8_t(1)))
	}
	return int(C.set_bucket_aggregate(d.handle, C.int8_t(0)))
}

func (d *Driver) SetBucketAggregateBuckets(buckets uint64) int {
	return int(C.set_bucket_aggregate_buckets(d.handle, C.size_t(buckets)))
}

func (d *Driver) HasNextPipeline() bool {
	return C.has_next_pipeline(d.handle) != 0
}

func (d *Driver) PreparePipeline() int {
	return int(C.prepare_pipeline(d.handle))
}

func (d *Driver) IsPipelineFinal() bool {
	return C.is_pipeline_final(d.handle) != 0
}

func (d *Driver) FinishPipeline() int {
	return int(C.finish_pipeline(d.handle))
}

func (d *Driver) PipelineHasNextSource() bool {
	return C.pipeline_has_next_source(d.handle) != 0
}

func (d *Driver) PipelineNextSource() int64 {
	return int64(C.pipeline_next_source(d.handle))
}

func (d *Driver) IsHeapSource(sourceId int64) bool {
	return C.is_heap_source(C.int64_t(sourceId)) != 0
}

func (d *Driver) PipelinePush(threadId int64, sourceId int64, record *array.Record) (int, *InputRecord) {
	inputRecord := fromGoRecord(record)
	return int(C.pipeline_push(d.handle, C.int64_t(threadId), C.int64_t(sourceId), inputRecord.cArray, inputRecord.cSchema)), inputRecord
}

func (d *Driver) PipelineStream(threadId int64, sourceId int64, record *array.Record, rows uint64) (int, *OutputRecord) {
	inputRecord := fromGoRecord(record)
	var outCSchema C.ArrowSchema
	var outCArray C.ArrowArray
	res := int(C.pipeline_stream(d.handle, C.int64_t(threadId), C.int64_t(sourceId), inputRecord.cArray, inputRecord.cSchema, &outCArray, &outCSchema, C.size_t(rows)))
	if res <= 0 {
		return res, nil
	}
	return res, toGoRecord(&outCSchema, &outCArray)
}
