package kaiwudb

import (
	"time"

	"github.com/benchant/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/benchant/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/benchant/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/benchant/tsbs/pkg/query"
)

// BaseGenerator contains settings specific for Influx database.
type BaseGenerator struct {
	ReadingDBName     string
	DiagnosticsDBName string
	CPUDBName         string
}

func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewKaiwudb()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, table, sql string) {
	q := qi.(*query.Kaiwudb)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Hypertable = []byte(table)
	q.SqlQuery = []byte(sql)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	iot := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return iot, nil
}
