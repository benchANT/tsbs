package main

import (
	"fmt"
	"github.com/apache/iotdb-client-go/common"
	"log"
	"strings"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"

	"github.com/apache/iotdb-client-go/client"
)

// database option vars
var (
	clientConfig    client.Config
	timeoutInMs     int64 // 0 for no timeout
	usingGroupByApi bool  // if using group by api when executing query
	singleDatabase  bool  // if using single database, e.g. only one database: root.db. root.db.cpu, root.db.mem belongs to this databse
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("host", "localhost", "Hostname of IoTDB instance")
	pflag.String("port", "6667", "Which port to connect to on the database host")
	pflag.String("user", "root", "The user who connect to IoTDB")
	pflag.String("password", "root", "The password for user connecting to IoTDB")
	pflag.Bool("use-groupby", false, "Whether to use group by api")
	pflag.Bool("single-database", false, "Whether to use single database")

	pflag.Parse()

	err := utils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	host := viper.GetString("host")
	port := viper.GetString("port")
	user := viper.GetString("user")
	password := viper.GetString("password")
	workers := viper.GetUint("workers")
	usingGroupByApi = viper.GetBool("use-groupby")
	singleDatabase = viper.GetBool("single-database")
	timeoutInMs = 0

	log.Printf("tsbs_run_queries_iotdb target: %s:%s. Loading with %d workers.\n", host, port, workers)
	if workers < 5 {
		log.Println("Insertion throughput is strongly related to the number of threads. Use more workers for better performance.")
	}

	clientConfig = client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}

	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.IoTDBPool, newProcessor)
}

type processor struct {
	session        client.Session
	printResponses bool
	groupByQuery   bool
}

func newProcessor() query.Processor { return &processor{} }

func (p *processor) Init(workerNumber int) {
	p.session = client.NewSession(&clientConfig)
	p.printResponses = runner.DoPrintResponses()
	if err := p.session.Open(false, int(timeoutInMs)); err != nil {
		errMsg := fmt.Sprintf("query processor init error, session is not open: %v\n", err)
		errMsg = errMsg + fmt.Sprintf("timeout setting: %d ms", timeoutInMs)
		log.Fatal(errMsg)
	}
}

func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	iotdbQ := q.(*query.IoTDB)
	sql := string(iotdbQ.SqlQuery)
	aggregatePaths := iotdbQ.AggregatePaths
	var interval int64 = 60000
	var startTimeInMills = iotdbQ.StartTime
	var endTimeInMills = iotdbQ.EndTime
	var dataSet *client.SessionDataSet
	var legalNodes = true
	var err error

	start := time.Now().UnixNano()
	if startTimeInMills > 0 {
		if usingGroupByApi {
			splits := strings.Split(aggregatePaths[0], ".")
			db := splits[0] + "." + splits[1]
			device := strings.Join(splits[:len(splits)-1], ".")
			measurement := splits[len(splits)-1]
			fmt.Println(db, device, measurement)
			dataSet, err = p.session.ExecuteGroupByQueryIntervalQuery(&db, device, measurement,
				common.TAggregationType_MAX_VALUE, 2,
				&startTimeInMills, &endTimeInMills, &interval, &timeoutInMs)
		} else {
			dataSet, err = p.session.ExecuteAggregationQueryWithLegalNodes(aggregatePaths,
				[]common.TAggregationType{common.TAggregationType_MAX_VALUE},
				&startTimeInMills, &endTimeInMills, &interval, &timeoutInMs, &legalNodes)
		}
	} else {
		dataSet, err = p.session.ExecuteQueryStatement(sql, &timeoutInMs)
	}

	if err == nil {
		if p.printResponses {
			if startTimeInMills > 0 {
				sql = fmt.Sprintf("SELECT MAX_VALUE(%s) GROUP BY ([%d, %d), %d)",
					iotdbQ.AggregatePaths, iotdbQ.StartTime, iotdbQ.EndTime, interval)
			}
			printDataSet(sql, dataSet)
		} else {
			// var next bool
			// for next, err = dataSet.Next(); err == nil && next; next, err = dataSet.Next() {
			// 	// Traverse query results
			// }
		}
	}
	took := time.Now().UnixNano() - start

	defer dataSet.Close()

	if err != nil {
		if startTimeInMills > 0 {
			sql = fmt.Sprintf("SELECT MAX_VALUE(%s) GROUP BY ([%d, %d), %d)",
				iotdbQ.SqlQuery, iotdbQ.StartTime, iotdbQ.EndTime, interval)
		}
		log.Printf("An error occurred while executing query SQL: %s\n", sql)
		return nil, err
	}

	lag := float64(took) / float64(time.Millisecond) // in milliseconds
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, err
}

func printDataSet(sql string, sds *client.SessionDataSet) {
	fmt.Printf("\nResponse for query '%s':\n", sql)
	showTimestamp := !sds.IsIgnoreTimeStamp()
	if showTimestamp {
		fmt.Print("Time\t\t\t\t")
	}

	for i := 0; i < sds.GetColumnCount(); i++ {
		fmt.Printf("%s\t", sds.GetColumnName(i))
	}
	fmt.Println()

	printedColsCount := 0
	for next, err := sds.Next(); err == nil && next; next, err = sds.Next() {
		if showTimestamp {
			fmt.Printf("%s\t", sds.GetText(client.TimestampColumnName))
		}
		for i := 0; i < sds.GetColumnCount(); i++ {
			columnName := sds.GetColumnName(i)
			v := sds.GetValue(columnName)
			if v == nil {
				v = "null"
			}
			fmt.Printf("%v\t\t", v)
		}
		fmt.Println()
		printedColsCount++
	}
	if printedColsCount == 0 {
		fmt.Println("Empty Set.")
	}
}
