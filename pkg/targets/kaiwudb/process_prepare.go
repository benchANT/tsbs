package kaiwudb

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/benchant/tsbs/pkg/targets"
	"github.com/benchant/tsbs/pkg/targets/kaiwudb/commonpool"
)

const microSecFromUnixEpochToY2K = 946684800 * 1000000
const bufferRows = 50

type fixedArgList struct {
	args     [][]byte
	capacity int
	writePos int
}

func newFixedArgList(capacity int) *fixedArgList {
	return &fixedArgList{
		args:     make([][]byte, capacity),
		capacity: capacity,
		writePos: 0,
	}
}

func (fa *fixedArgList) Init() {
	for i := 0; i < fa.capacity; i++ {
		fa.args[i] = make([]byte, 8)
	}
}

func (fa *fixedArgList) Reset() {
	fa.writePos = 0
}

func (fa *fixedArgList) Append(value []byte) {
	fa.args[fa.writePos] = value
	fa.writePos++
}

func (fa *fixedArgList) Emplace(value uint64) {
	binary.BigEndian.PutUint64(fa.args[fa.writePos], value)
	fa.writePos++
}

func (fa *fixedArgList) Capacity() int {
	return fa.capacity
}

func (fa *fixedArgList) Length() int {
	return fa.writePos
}

func (fa *fixedArgList) Args() [][]byte {
	return fa.args[:fa.writePos]
}

type prepareProcessor struct {
	opts        *LoadingOptions
	dbName      string
	sci         *syncCSI
	_db         *commonpool.Conn
	preparedSql map[string]struct{}
	workerIndex int

	// prepare buff
	buffer     map[string]*fixedArgList // tableName, fixedArgList
	buffInited bool
	wg         *sync.WaitGroup
}

func newProcessorPrepare(opts *LoadingOptions, dbName string) *prepareProcessor {
	return &prepareProcessor{
		opts:        opts,
		dbName:      dbName,
		sci:         globalSCI,
		preparedSql: make(map[string]struct{}),
		buffer:      make(map[string]*fixedArgList),
		wg:          &sync.WaitGroup{},
	}
}

func (p *prepareProcessor) Init(workerNum int, doLoad, _ bool) {
	if !doLoad {
		return
	}

	p.workerIndex = workerNum

	var err error
	l := len(p.opts.Port)
	idx := workerNum % l
	p._db, err = commonpool.GetConnection(p.opts.User, p.opts.Pass, p.opts.Host[idx], p.opts.Port[idx], 0)
	if err != nil {
		panic(err)
	}
}

func (p *prepareProcessor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batches := b.(*hypertableArr)
	rowCnt := uint64(0)
	var deviceNum, deviceMetric uint64
	metricCnt := batches.totalMetric
	if !doLoad {
		if len(batches.createSql) != 0 {
			for _, row := range batches.createSql {
				switch row.sqlType {
				case CreateTable:
				case InsertMetricAndTag:
					deviceNum += 1
					rowCnt += 1
					metricCnt += uint64(batches.cols[row.table] - 2)
				}
			}
		}

		for _, sqls := range batches.m {
			rowCnt += uint64(len(sqls))
		}
		return metricCnt, rowCnt
	}

	// create table
	if p.opts.DoCreate && len(batches.createSql) != 0 {
		deviceNum, deviceMetric = p.createDeviceAndAttribute(batches)
		rowCnt += deviceNum
		metricCnt += deviceMetric
	} else {
		deviceNum = 0
	}

	p.wg.Add(len(batches.devices))
	for deviceName := range batches.devices {
		device := deviceName
		go func() {
			defer p.wg.Done()
			_, ok := GlobalTable.Load(device)
			if ok {
				return
			}
			v, ok := p.sci.m.Load(device)
			if ok {
				<-v.(*Ctx).c.Done()
				return
			}
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(device, ctx)
			<-actual.(*Ctx).c.Done()
			return
		}()
	}
	p.wg.Wait()

	// join args and execute
	for name, args := range batches.m {
		rowCnt += uint64(len(args))
		tableName := strings.Split(name, ":")[0]
		// init buffer for every table
		tableBuffer, ok := p.buffer[tableName]
		if !ok {
			bufferSize := bufferRows * batches.cols[tableName]
			tableBuffer = newFixedArgList(bufferSize)
			tableBuffer.Init()
			p.buffer[tableName] = tableBuffer
		}

		var formatBuf []int16
		for _, s := range args {
			s = s[1 : len(s)-1]
			values := strings.Split(s, ",")

			// Emplace
			for i, v := range values {
				if i < (batches.cols[tableName] - 1) {
					num, _ := strconv.ParseInt(v, 10, 64)
					if i == 0 {
						// timestamp: UTC+8 Time Zone
						tableBuffer.Emplace(uint64(num*1000) - microSecFromUnixEpochToY2K)
					} else {
						// row data
						tableBuffer.Emplace(uint64(num))
					}
				} else {
					v = strings.TrimSpace(v)
					vv := strings.Split(v, "'")
					tableBuffer.Append([]byte(vv[1]))
				}
				formatBuf = append(formatBuf, 1)
			}
			if tableBuffer.Length() == tableBuffer.Capacity() {
				key := p.createPrepareSql(tableName, tableBuffer.Capacity())
				p.execPrepareStmt(key, tableBuffer.args, formatBuf)
				// reuse buffer: reset tableBuffer's write position
				tableBuffer.Reset()
				formatBuf = []int16{}
			}
		}
		// check buffer is full
		if tableBuffer.Length() > 0 {
			key := p.createPrepareSql(tableName, tableBuffer.Length())
			p.execPrepareStmt(key, tableBuffer.Args(), formatBuf)
			// reuse buffer: reset tableBuffer's write position
			tableBuffer.Reset()
			formatBuf = []int16{}
		}
	}

	batches.Reset()
	return metricCnt, rowCnt
}

func (p *prepareProcessor) Close(doLoad bool) {
	if doLoad {
		p._db.Put()
	}
}

func (p *prepareProcessor) createDeviceAndAttribute(batches *hypertableArr) (uint64, uint64) {
	var deviceNum uint64 = 0
	var deviceMetricNum uint64 = 0
	for _, row := range batches.createSql {
		switch row.sqlType {
		case CreateTable:
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(row.table, ctx)
			sql := strings.ReplaceAll(
				row.sql,
				fmt.Sprintf("create table %s", row.table),
				fmt.Sprintf("create table %s.%s", p.opts.DBName, row.table))

			_, err := p._db.Connection.Exec(ctx.c, sql)
			if err != nil {
				panic(fmt.Sprintf("kaiwudb2.0 create device failed,err :%s", err))
			}

			if err != nil && !strings.Contains(err.Error(), "already exists") {
				panic(fmt.Sprintf("kaiwudb2.0 create device failed,err :%s", err))
			}
			GlobalTable.Store(row.table, row.cols)
			actual.(*Ctx).cancel()
		case InsertMetricAndTag:
			deviceNum += 1
			deviceMetricNum += uint64(batches.cols[row.table] - 2)
			c, cancel := context.WithCancel(context.Background())
			ctx := &Ctx{
				c:      c,
				cancel: cancel,
			}
			actual, _ := p.sci.m.LoadOrStore(row.device, ctx)
			//check if table created
			_, ok := GlobalTable.Load(row.table)
			if !ok {
				v, ok := p.sci.m.Load(row.table)
				if ok {
					<-v.(*Ctx).c.Done()
					sql := "insert into " + p.dbName + "." + row.table + " values " + row.sql
					fmt.Println(sql)
					_, err := p._db.Connection.Exec(context.Background(), sql)
					if err != nil {
						panic(fmt.Sprintf("kaiwudb2.0 insert data failed,err :%s", err))
					}

					GlobalTable.Store(row.device, nothing)
					actual.(*Ctx).cancel()
					continue
				}
				// wait for table created
				tableC, tableCancel := context.WithCancel(context.Background())
				tableCtx := &Ctx{
					c:      tableC,
					cancel: tableCancel,
				}
				tableActual, _ := p.sci.m.LoadOrStore(row.table, tableCtx)
				<-tableActual.(*Ctx).c.Done()
			}

			sql := fmt.Sprintf("insert into %s.%s values %s", p.dbName, row.table, row.sql)
			_, err := p._db.Connection.Exec(context.Background(), sql)
			if err != nil {
				panic(fmt.Sprintf("kaiwudb2.0 insert data failed,err :%s", err))
			}
			GlobalTable.Store(row.device, nothing)
			actual.(*Ctx).cancel()
			continue

		default:
			panic("impossible")
		}
	}
	return deviceNum, deviceMetricNum
}

func (p *prepareProcessor) createPrepareSql(tableName string, args int) string {
	key := fmt.Sprintf("tsbs-insert-%s-%d", tableName, args)
	_, ok := p.preparedSql[key]
	if ok {
		return key
	}

	v, ok := GlobalTable.Load(tableName)
	if !ok {
		v1, ok1 := p.sci.m.Load(tableName)
		if ok1 {
			<-v1.(*Ctx).c.Done()
		}
		// wait for table created
		tableC, tableCancel := context.WithCancel(context.Background())
		tableCtx := &Ctx{
			c:      tableC,
			cancel: tableCancel,
		}
		tableActual, _ := p.sci.m.LoadOrStore(tableName, tableCtx)
		<-tableActual.(*Ctx).c.Done()
		return p.createPrepareSql(tableName, args)
	}
	cols := v.(string)

	simpleColumns := len(strings.Split(cols, ","))
	var insertsql strings.Builder

	query := fmt.Sprintf("insert into %s.%s %s values ", p.opts.DBName, tableName, cols)
	insertsql.WriteString(query)

	for i := 1; i <= args; i++ {
		if i%(simpleColumns) == 1 {
			insertsql.WriteString("(")
		}
		insertsql.WriteString(fmt.Sprintf("$%d", i))

		if i%(simpleColumns) == 0 && i < args {
			insertsql.WriteString("),")
		} else if i%(simpleColumns) != 0 && i < args {
			insertsql.WriteString(",")
		}
		if i == args {
			insertsql.WriteString(");")
		}
	}

	sql := insertsql.String()

	// fmt.Println(sql)

	_, err1 := p._db.Connection.Prepare(context.Background(), key, sql)
	if err1 != nil {
		panic(fmt.Sprintf("kaiwudb Prepare failed,err :%s, sql :%s", err1, sql))
	}

	p.preparedSql[key] = struct{}{}
	return key
}

func (p *prepareProcessor) execPrepareStmt(key string, args [][]byte, formatBuf []int16) {
	res := p._db.Connection.PgConn().ExecPrepared(context.Background(), key, args, formatBuf, []int16{}).Read()
	if res.Err != nil {
		panic(res.Err)
	}
}
