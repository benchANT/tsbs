package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/benchant/tsbs/internal/utils"
	"github.com/benchant/tsbs/pkg/query"
	"github.com/benchant/tsbs/pkg/targets/kaiwudb"
	"github.com/benchant/tsbs/pkg/targets/kaiwudb/commonpool"
	"github.com/blagojts/viper"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

var (
	user   string
	pass   string
	host   string
	port   int
	format int
	mode   int
	runner *query.BenchmarkRunner
)

func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("user", "root", "User to connect to kaiwudb")
	pflag.String("pass", "", "Password for the user connecting to kaiwudb")
	pflag.String("host", "", "kaiwudb host")
	pflag.Int("port", 36257, "kaiwudb Port")
	pflag.Int("query-mode", int(pgx.QueryExecModeSimpleProtocol), "kaiwudb pgx query mode")
	pflag.Parse()
	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
	user = viper.GetString("user")
	pass = viper.GetString("pass")
	host = viper.GetString("host")
	port = viper.GetInt("port")
	mode = viper.GetInt("query-mode")
	runner = query.NewBenchmarkRunner(config)
	format = 0
}
func main() {
	runner.Run(&query.KaiwudbPool, newProcessor)
}

type queryExecutorOptions struct {
	debug         bool
	printResponse bool
}

type processor struct {
	db        *commonpool.Conn
	opts      *queryExecutorOptions
	queryMode pgx.QueryExecMode
	parse     *kaiwudb.ParsePrepare
}

func (p *processor) Init(workerNum int) {
	db, err := commonpool.GetConnection(user, pass, host, port, format)
	if err != nil {
		panic(err)
	}
	p.db = db
	p.opts = &queryExecutorOptions{
		debug:         runner.DebugLevel() > 0,
		printResponse: runner.DoPrintResponses(),
	}

	p.queryMode = pgx.QueryExecMode(mode)
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	tq := q.(*query.Kaiwudb)

	start := time.Now()
	qry := string(tq.SqlQuery)
	ctx := context.Background()

	var rows pgx.Rows
	var err error
	switch p.queryMode {
	case pgx.QueryExecModeCacheStatement:
		sql, args := kaiwudb.HostRangeFast(qry, p.opts.debug)
		args = append([]interface{}{p.queryMode}, args...)
		rows, err = p.db.Connection.Query(ctx, sql, args...)
	case pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec:
		if p.parse == nil {
			p.parse = kaiwudb.NewParsePrepare(string(tq.HumanLabel))
		}
		sql, args := p.parse.Parse(qry, p.opts.debug)
		args = append([]interface{}{p.queryMode}, args...)
		rows, err = p.db.Connection.Query(ctx, sql, args...)
	default:
		rows, err = p.db.Connection.Query(ctx, qry, p.queryMode)
	}

	if err != nil {
		log.Println("Error running query: '", qry, "'")
		return nil, err
	}

	if p.opts.printResponse {
		prettyPrintResponse(rows, qry)
	}
	for rows.Next() {

	}
	rows.Close()

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)

	if p.opts.debug {
		fmt.Println(qry, took)
	}

	return []*query.Stat{stat}, nil
}

func newProcessor() query.Processor { return &processor{} }

func prettyPrintResponse(rows pgx.Rows, query string) {
	resp := make(map[string]interface{})
	resp["query"] = query
	resp["results"] = mapRows(rows)

	line, err := json.MarshalIndent(resp, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(line) + "\n")
}

func mapRows(r pgx.Rows) []map[string]interface{} {
	rows := []map[string]interface{}{}
	cols := r.FieldDescriptions()
	for r.Next() {
		row := make(map[string]interface{})
		values := make([]interface{}, len(cols))
		for i := range values {
			values[i] = new(interface{})
		}

		err := r.Scan(values...)
		if err != nil {
			panic(errors.Wrap(err, "error while reading values"))
		}

		for i, column := range cols {
			row[column.Name] = *values[i].(*interface{})
		}
		rows = append(rows, row)
	}
	return rows
}
