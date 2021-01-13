package cura

// #include "c/bridge.h"
import (
	"C"
)

import (
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"runtime"
	"unsafe"
)

func getSchemaPointer(cSchemas []*C.ArrowSchema) unsafe.Pointer {
	if len(cSchemas) == 0 {
		return nil
	}
	return unsafe.Pointer(&cSchemas[0])
}

func getArrayPointer(cArrays []*C.ArrowArray) unsafe.Pointer {
	if len(cArrays) == 0 {
		return nil
	}
	return unsafe.Pointer(&cArrays[0])
}

func getBuffersPointer(buffers []uintptr) unsafe.Pointer {
	if len(buffers) == 0 {
		return nil
	}
	return unsafe.Pointer(&buffers[0])
}

func getBufferPointer(buffer []byte) uintptr {
	if len(buffer) == 0 {
		return uintptr(unsafe.Pointer(nil))
	}
	return uintptr(unsafe.Pointer(&buffer[0]))
}

func newCSchema(format string, name string, flags int64, children []*C.ArrowSchema) *C.ArrowSchema {
	cSchema := C.new_schema(C.CString(format), C.CString(name), C.int64_t(flags), C.int64_t(len(children)),
		(**C.ArrowSchema)(getSchemaPointer(children)))
	return cSchema
}

func newCArray(length int64, nullCount int64, offset int64, buffers []uintptr, children []*C.ArrowArray) *C.ArrowArray {
	cArray := C.new_array(C.int64_t(length), C.int64_t(nullCount), C.int64_t(offset),
		C.int64_t(len(buffers)), (*unsafe.Pointer)(getBuffersPointer(buffers)),
		C.int64_t(len(children)), (**C.ArrowArray)(getArrayPointer(children)))
	return cArray
}

type InputRecord struct {
	array.Record
	cSchema *C.ArrowSchema
	cArray  *C.ArrowArray
}

func releaseInputRecord(record *InputRecord) {
	C.delete_schema(record.cSchema)
	C.delete_array(record.cArray)
}

type OutputRecord struct {
	array.Record
	cSchema *C.ArrowSchema
	cArray  *C.ArrowArray
}

func releaseOutputRecord(record *OutputRecord) {
	C.release_schema(record.cSchema)
	C.release_array(record.cArray)
}

func fromGoSchema(schema *arrow.Schema) *C.ArrowSchema {
	if schema == nil {
		return nil
	}

	children := make([]*C.ArrowSchema, len(schema.Fields()))
	for i, field := range schema.Fields() {
		var format string
		switch field.Type.ID() {
		case arrow.BOOL:
			format = "b"
		case arrow.INT8:
			format = "c"
		case arrow.UINT8:
			format = "C"
		case arrow.INT16:
			format = "s"
		case arrow.UINT16:
			format = "S"
		case arrow.INT32:
			format = "i"
		case arrow.UINT32:
			format = "I"
		case arrow.INT64:
			format = "l"
		case arrow.UINT64:
			format = "L"
		case arrow.FLOAT32:
			format = "f"
		case arrow.FLOAT64:
			format = "g"
		case arrow.STRING:
			format = "u"
		default:
			return nil
		}
		var flags int64 = 0
		if field.Nullable {
			flags |= 2
		}
		children[i] = newCSchema(format, field.Name, flags, make([]*C.ArrowSchema, 0))
	}
	return newCSchema("+s", "", 0, children)
}

func fromGoRecord(record *array.Record) *InputRecord {
	if record == nil {
		return &InputRecord{nil, nil, nil}
	}

	children := make([]*C.ArrowArray, (*record).NumCols())
	for i, column := range (*record).Columns() {
		buffers := make([]uintptr, len(column.Data().Buffers()))
		for j, buffer := range column.Data().Buffers() {
			buffers[j] = getBufferPointer(buffer.Buf())
		}
		children[i] = newCArray(int64(column.Len()), -1, int64(column.Data().Offset()), buffers, make([]*C.ArrowArray, 0))
	}
	buffers := make([]uintptr, 1)
	buffers[0] = uintptr(unsafe.Pointer(nil))

	cSchema, cArray := fromGoSchema((*record).Schema()), newCArray((*record).NumRows(), -1, 0, buffers, children)
	inputRecord := &InputRecord{*record, cSchema, cArray}
	runtime.SetFinalizer(inputRecord, releaseInputRecord)
	return inputRecord
}

func toGoArray(cSchema *C.ArrowSchema, cArray *C.ArrowArray) (arrow.Field, array.Interface) {
	var dataType arrow.DataType
	format := C.GoString(cSchema.format)
	if format == "b" {
		dataType = arrow.FixedWidthTypes.Boolean
	} else if format == "c" {
		dataType = arrow.PrimitiveTypes.Int8
	} else if format == "C" {
		dataType = arrow.PrimitiveTypes.Uint8
	} else if format == "s" {
		dataType = arrow.PrimitiveTypes.Int16
	} else if format == "S" {
		dataType = arrow.PrimitiveTypes.Uint16
	} else if format == "i" {
		dataType = arrow.PrimitiveTypes.Int32
	} else if format == "I" {
		dataType = arrow.PrimitiveTypes.Uint32
	} else if format == "l" {
		dataType = arrow.PrimitiveTypes.Int64
	} else if format == "L" {
		dataType = arrow.PrimitiveTypes.Uint64
	} else if format == "f" {
		dataType = arrow.PrimitiveTypes.Float32
	} else if format == "g" {
		dataType = arrow.PrimitiveTypes.Float64
	} else if format == "u" {
		dataType = arrow.BinaryTypes.String
	} else {
		return arrow.Field{}, nil
	}
	nBuffers := int(cArray.n_buffers)
	cBuffers := (*[1 << 28]unsafe.Pointer)(unsafe.Pointer(cArray.buffers))[:nBuffers:nBuffers]
	buffers := make([]*memory.Buffer, nBuffers)
	for i := 0; i < nBuffers; i++ {
		if cBuffers[i] == nil {
			buffers[i] = nil
		} else {
			buffers[i] = memory.NewBufferBytes((*[1 << 28]byte)(cBuffers[i])[0:])
			// TODO: Maybe unnecessary, this `Retain()` call keeps the actual memory from being released as it's managed by C.
			buffers[i].Retain()
		}
	}
	data := array.NewData(dataType, int(cArray.length), buffers, make([]*array.Data, 0), -1, int(cArray.offset))
	return arrow.Field{Name: "", Type: dataType, Nullable: int64(cSchema.flags)&1 == 0}, array.MakeFromData(data)
}

func toGoRecord(cSchema *C.ArrowSchema, cArray *C.ArrowArray) *OutputRecord {
	nChildren := int(cSchema.n_children)
	schemaChildren := (*[1 << 28]*C.ArrowSchema)(unsafe.Pointer(cSchema.children))[:nChildren:nChildren]
	arrayChildren := (*[1 << 28]*C.ArrowArray)(unsafe.Pointer(cArray.children))[:nChildren:nChildren]
	fields := make([]arrow.Field, nChildren)
	children := make([]array.Interface, nChildren)
	for i := 0; i < int(cSchema.n_children); i++ {
		field, child := toGoArray(schemaChildren[i], arrayChildren[i])
		fields[i] = field
		children[i] = child
	}
	schema := arrow.NewSchema(fields, nil)
	record := &OutputRecord{array.NewRecord(schema, children, int64(cArray.length)), cSchema, cArray}
	runtime.SetFinalizer(record, releaseOutputRecord)
	return record
}
