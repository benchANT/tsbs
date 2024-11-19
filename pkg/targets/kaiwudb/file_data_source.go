package kaiwudb

import (
	"bufio"
	"strconv"
	"strings"

	"github.com/benchant/tsbs/load"
	"github.com/benchant/tsbs/pkg/data"
	"github.com/benchant/tsbs/pkg/data/usecases/common"
	"github.com/benchant/tsbs/pkg/targets"
)

func newFileDataSource(fileName string) targets.DataSource {
	br := load.GetBufferedReader(fileName)

	return &fileDataSource{scanner: bufio.NewScanner(br)}
}

type fileDataSource struct {
	scanner *bufio.Scanner
	headers *common.GeneratedDataHeaders
}

func (d *fileDataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

func (d *fileDataSource) NextItem() data.LoadedPoint {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return data.LoadedPoint{}
	} else if !ok {
		fatal("scan error: %v", d.scanner.Err())
		return data.LoadedPoint{}
	}
	p := &point{}
	line := d.scanner.Text()
	p.sqlType = line[0]
	switch line[0] {
	case InsertMetric:
		parts := strings.SplitN(line, ",", 5)
		p.table = parts[1]  // cpu
		p.device = parts[2] // host_0
		// p.tag = parts[2]
		p.fieldCount, _ = strconv.Atoi(parts[3])
		p.sql = strings.TrimSpace(parts[4])

	case CreateTable:
		parts := strings.SplitN(line, ",", 4)
		p.table = parts[1] //cpu
		// p.device = parts[2]   //host_0
		p.cols = strings.Replace(parts[2], ":", ",", -1)
		p.sql = parts[3] //(column) tags (tagStr)
	case InsertMetricAndTag:
		parts := strings.SplitN(line, ",", 4)
		p.table = parts[1]  //cpu
		p.device = parts[2] //host_0
		p.sql = parts[3]    //metrics + tags
	//case Modify:
	//	parts := strings.SplitN(line, ",", 4)
	//	p.superTable = parts[1]
	//	p.subTable = parts[2]
	//	p.sql = parts[3]
	default:
		panic(line)
	}
	return data.NewLoadedPoint(p)
}
