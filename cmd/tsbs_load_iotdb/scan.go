package main

import (
	"bufio"
	"github.com/benchant/tsbs/pkg/data"
	"github.com/benchant/tsbs/pkg/data/usecases/common"
	"github.com/benchant/tsbs/pkg/targets"
	"github.com/benchant/tsbs/pkg/targets/iotdb"
	"strings"
)

// iotdbPoint is a single record(row) of data
type iotdbPoint struct {
	db        string
	deviceID  string
	values    string
	fieldsCnt int
}

// A struct that stores data points
type iotdbBatch struct {
	m          map[string][]string
	rowCnt     uint   // count of records(rows)
	metricsCnt uint64 // total count of all metrics in this batch
}

type fileDataSource struct {
	scanner *bufio.Scanner
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		return data.LoadedPoint{}
	}

	// return data.NewLoadedPoint(d.scanner.Bytes())

	line := d.scanner.Text()

	// deviceID and rest values of fields
	lineParts := strings.SplitN(line, ",", 4)
	opType := lineParts[0]
	if opType == "0" {
		return data.NewLoadedPoint(
			&iotdbPoint{
				db:       lineParts[1],
				deviceID: lineParts[1] + "." + lineParts[2],
				values:   lineParts[3],
			})
	} else {
		return data.NewLoadedPoint(
			&iotdbPoint{
				db:        lineParts[1],
				deviceID:  lineParts[1] + "." + lineParts[2],
				values:    lineParts[3],
				fieldsCnt: len(iotdb.GlobalDataTypeMap[lineParts[1]]),
			})
	}
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders { return nil }

func (batch *iotdbBatch) Len() uint {
	return batch.rowCnt
}

func (batch *iotdbBatch) Append(item data.LoadedPoint) {
	that := item.Data.(*iotdbPoint)
	batch.rowCnt++
	batch.metricsCnt += uint64(that.fieldsCnt)
	batch.m[that.deviceID] = append(batch.m[that.deviceID], that.values)
}

func (batch *iotdbBatch) Reset() {
	batch.rowCnt = 0
	batch.metricsCnt = 0
	batch.m = map[string][]string{}
}

type factory struct{}

func (f *factory) New() targets.Batch {
	return &iotdbBatch{
		m:          map[string][]string{},
		rowCnt:     0,
		metricsCnt: 0,
	}
}
