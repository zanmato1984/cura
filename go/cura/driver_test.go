package cura

import (
	"fmt"
	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"sync"
	"testing"
	"unsafe"
)

const parallelism = 8
const rows = uint64(5)

type table struct {
	id      int64
	records []array.Record
}

func makeTable(id int64, records []array.Record) table {
	return table{id, records}
}

type db struct {
	tables map[int64]table
}

func makeDb(tables []table) db {
	tableMap := make(map[int64]table)
	for _, table := range tables {
		tableMap[table.id] = table
	}
	return db{tableMap}
}

func (d *db) execute(t *testing.T, json string) {
	readers := make(map[int64]op)
	for _, table := range d.tables {
		readers[table.id] = makeTableReader(table)
	}
	cura := makeCura(readers, json)
	sink := makeSink(cura)
	sink.open(t)
	sink.sink(t)
	sink.close(t)
}

type op interface {
	open(t *testing.T)
	next(t *testing.T) (*array.Record, unsafe.Pointer)
	close(t *testing.T)
}

type tableReader struct {
	op
	table table
	wg    sync.WaitGroup
	c     chan *array.Record
}

func makeTableReader(table table) *tableReader {
	return &tableReader{table: table, c: make(chan *array.Record)}
}

func (tr *tableReader) open(*testing.T) {
	tr.wg.Add(len(tr.table.records))
	for i := range tr.table.records {
		i := i
		go func() {
			defer tr.wg.Done()
			tr.c <- &tr.table.records[i]
		}()
	}
	go func() {
		tr.wg.Wait()
		tr.c <- nil
	}()
}

func (tr *tableReader) next(*testing.T) (*array.Record, unsafe.Pointer) {
	record, more := <-tr.c
	if !more {
		return nil, nil
	}
	if record == nil {
		close(tr.c)
	}
	return record, nil
}

func (tr *tableReader) close(*testing.T) {
}

type cura struct {
	op
	children map[int64]op
	json     string
	c        chan *OutputRecord

	driver *Driver
}

func makeCura(children map[int64]op, json string) *cura {
	return &cura{children: children, json: json, c: make(chan *OutputRecord)}
}

func (c *cura) open(t *testing.T) {
	for _, child := range c.children {
		child.open(t)
	}
	c.driver = CreateDriver()
	if c.driver == nil {
		t.Fatal("create driver failed")
	}
	err, explained := c.driver.Explain(c.json, true)
	if err <= 0 {
		t.Fatal("explain cura failed")
	} else {
		for _, line := range explained {
			fmt.Println(line)
		}
	}

	err = c.driver.Compile(c.json)
	if err != 0 {
		t.Fatal("compile cura failed")
	}

	go func() {
		for c.driver.HasNextPipeline() {
			c.driver.PreparePipeline()
			if err != 0 {
				t.Fatal("prepare cura pipeline failed")
			}
			var wg sync.WaitGroup
			// Reference all the input records pushed to current pipeline to preserve life cycle until this pipeline finishes.
			inputRecords := make([][]*InputRecord, 0)
			final := c.driver.IsPipelineFinal()
			if final {
				fmt.Println("Executing cura final pipeline")
				for c.driver.PipelineHasNextSource() {
					wg.Add(parallelism)
					sourceId := c.driver.PipelineNextSource()
					if c.driver.IsHeapSource(sourceId) {
						fmt.Println("Pipelining heap source", sourceId)
						// `parallelism` goroutines each processing groups of `rows` rows.
						for i := 0; i < parallelism; i++ {
							go func() {
								defer wg.Done()
								var res, outRecord = c.driver.PipelineStream(-1, sourceId, nil, rows)
								if res < 0 {
									t.Fatal("cura pipeline stream failed")
								}
								for outRecord != nil {
									c.c <- outRecord
									res, outRecord = c.driver.PipelineStream(-1, sourceId, nil, rows)
									if res < 0 {
										t.Fatal("cura pipeline stream failed")
									}
								}
							}()
						}
					} else {
						fmt.Println("Pipelining input source", sourceId)
						child := c.children[sourceId]
						// `parallelism` goroutines each processing arbitrary records from child.
						for i := 0; i < parallelism; i++ {
							thread := i
							go func() {
								defer wg.Done()
								var input, _ = child.next(t)
								for input != nil {
									res, outRecord := c.driver.PipelineStream(int64(thread), sourceId, input, 0)
									if res < 0 {
										t.Fatal("cura pipeline stream failed")
									}
									c.c <- outRecord
									input, _ = child.next(t)
								}
							}()
						}
					}
				}
			} else {
				fmt.Println("Executing cura intermediate pipeline")
				for c.driver.PipelineHasNextSource() {
					sourceId := c.driver.PipelineNextSource()
					if c.driver.IsHeapSource(sourceId) {
						fmt.Println("Pipelining heap source", sourceId)
						wg.Add(1)
						// 1 goroutines processing groups the whole intermediate heap source.
						go func() {
							defer wg.Done()
							res, _ := c.driver.PipelinePush(-1, sourceId, nil)
							if res < 0 {
								t.Fatal("cura pipeline stream failed")
							}
						}()
					} else {
						fmt.Println("Pipelining input source", sourceId)
						wg.Add(parallelism)
						child := c.children[sourceId]
						// `parallelism` goroutines each processing arbitrary records from child.
						for i := 0; i < parallelism; i++ {
							thread := i
							n := len(inputRecords)
							inputRecords = append(inputRecords, make([]*InputRecord, 0))
							go func() {
								defer wg.Done()
								var input, _ = child.next(t)
								for input != nil {
									res, inputRecord := c.driver.PipelinePush(int64(thread), sourceId, input)
									if res < 0 {
										t.Fatal("cura pipeline stream failed")
									}
									inputRecords[n] = append(inputRecords[n], inputRecord)
									input, _ = child.next(t)
								}
							}()
						}
					}
				}
			}
			// Synchronize this pipeline.
			wg.Wait()
			err = c.driver.FinishPipeline()
			if err != 0 {
				t.Fatal("finish cura pipeline failed")
			}
		}
		c.c <- nil
	}()
}

func (c *cura) next(t *testing.T) (*array.Record, unsafe.Pointer) {
	record, more := <-c.c
	if !more {
		return nil, nil
	}
	if record == nil {
		close(c.c)
		return nil, nil
	}
	return &record.Record, unsafe.Pointer(record)
}

func (c *cura) close(t *testing.T) {
	ReleaseDriver(c.driver)
	for _, child := range c.children {
		child.close(t)
	}
}

type sink struct {
	op op
	wg sync.WaitGroup
}

func makeSink(op op) *sink {
	return &sink{op: op}
}

func (s *sink) open(t *testing.T) {
	s.op.open(t)
}

func (s *sink) sink(t *testing.T) {
	s.wg.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer s.wg.Done()
			var output, _ = s.op.next(t)
			for output != nil {
				fmt.Println("Sink:", *output)
				output, _ = s.op.next(t)
			}
		}()
	}
	s.wg.Wait()
}

func (s *sink) close(t *testing.T) {
	s.op.close(t)
}

func makeRecord() array.Record {
	pool := memory.NewGoAllocator()

	builder1 := array.NewInt64Builder(pool)
	defer builder1.Release()

	builder1.Append(1)
	builder1.Append(2)
	builder1.Append(3)
	builder1.AppendNull()
	builder1.AppendNull()
	builder1.Append(5)
	builder1.Append(5)
	builder1.Append(6)
	builder1.Append(6)
	builder1.Append(7)
	builder1.Append(7)
	builder1.Append(8)
	builder1.AppendNull()
	array1 := builder1.NewInt64Array()

	builder2 := array.NewStringBuilder(pool)
	defer builder2.Release()

	builder2.Append("ab")
	builder2.Append("cd")
	builder2.Append("ef")
	builder2.Append("gh")
	builder2.AppendNull()
	builder2.Append("ij")
	builder2.Append("ji")
	builder2.Append("kl")
	builder2.Append("kl")
	builder2.Append("mn")
	builder2.Append("mn")
	builder2.AppendNull()
	builder2.AppendNull()
	array2 := builder2.NewStringArray()

	fields := make([]arrow.Field, 2)
	fields[0] = arrow.Field{Name: "", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	fields[1] = arrow.Field{Name: "", Type: arrow.BinaryTypes.String, Nullable: true}
	schema := arrow.NewSchema(fields, nil)
	columns := make([]array.Interface, 2)
	columns[0] = array1
	columns[1] = array2

	record := array.NewRecord(schema, columns, 8)
	return record
}

func TestSingleInput(test *testing.T) {
	input := `
{"rels": [
{"rel_op": "InputSource", "source_id": 100, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]}
]}
`
	t := makeTable(100, []array.Record{makeRecord(), makeRecord(), makeRecord()})
	db := makeDb([]table{t})
	db.execute(test, input)
}

func TestFilter(test *testing.T) {
	filter := `
{"rels": [
{"rel_op": "InputSource", "source_id": 100, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "Filter", "condition": {"binary_op": "EQUAL", "type": {"type": "BOOL8", "nullable": true}, "operands": [{"col_ref": 0}, {"literal": 7, "type": "INT64"}]}}
]}
`
	t := makeTable(100, []array.Record{makeRecord(), makeRecord(), makeRecord()})
	db := makeDb([]table{t})
	db.execute(test, filter)
}

func TestSingleLevelUnion(test *testing.T) {
	union := `
{"rels": [
{"rel_op": "InputSource", "source_id": 100, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "InputSource", "source_id": 200, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "Union"}
]}
`
	t1 := makeTable(100, []array.Record{makeRecord(), makeRecord()})
	t2 := makeTable(200, []array.Record{makeRecord(), makeRecord()})
	db := makeDb([]table{t1, t2})
	db.execute(test, union)
}

func TestDoubleLevelUnion(test *testing.T) {
	union := `
{"rels": [
{"rel_op": "InputSource", "source_id": 100, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "InputSource", "source_id": 200, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "Union"},
{"rel_op": "InputSource", "source_id": 300, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "Union"}
]}
`
	t1 := makeTable(100, []array.Record{makeRecord(), makeRecord()})
	t2 := makeTable(200, []array.Record{makeRecord(), makeRecord()})
	t3 := makeTable(300, []array.Record{makeRecord(), makeRecord()})
	db := makeDb([]table{t1, t2, t3})
	db.execute(test, union)
}

func TestHashJoin(test *testing.T) {
	join := `
{"rels": [
{"rel_op": "InputSource", "source_id": 100, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "InputSource", "source_id": 200, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "HashJoin", "type": "INNER", "condition": {"binary_op": "EQUAL", "type": {"type": "BOOL8", "nullable": true}, "operands": [{"col_ref": 1}, {"col_ref": 3}]}}
]}
`
	t1 := makeTable(100, []array.Record{makeRecord()})
	t2 := makeTable(200, []array.Record{makeRecord()})
	db := makeDb([]table{t1, t2})
	db.execute(test, join)
}

func TestHashJoinTwoUnions(test *testing.T) {
	join := `
{"rels": [
{"rel_op": "InputSource", "source_id": 100, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "InputSource", "source_id": 200, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "Union"},
{"rel_op": "InputSource", "source_id": 300, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "InputSource", "source_id": 400, "schema": [{"type": "INT64", "nullable": true}, {"type": "STRING", "nullable": true}]},
{"rel_op": "Union"},
{"rel_op": "HashJoin", "type": "INNER", "condition": {"binary_op": "EQUAL", "type": {"type": "BOOL8", "nullable": true}, "operands": [{"col_ref": 1}, {"col_ref": 3}]}}
]}
`
	t1 := makeTable(100, []array.Record{makeRecord()})
	t2 := makeTable(200, []array.Record{makeRecord()})
	t3 := makeTable(300, []array.Record{makeRecord()})
	t4 := makeTable(400, []array.Record{makeRecord()})
	db := makeDb([]table{t1, t2, t3, t4})
	db.execute(test, join)
}
