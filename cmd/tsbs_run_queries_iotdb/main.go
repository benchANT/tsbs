package main

import (
	"fmt"
	"github.com/apache/iotdb-client-go/common"
	"log"
	"strings"
	"time"

	"github.com/benchant/tsbs/internal/utils"
	"github.com/benchant/tsbs/pkg/query"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"

	"github.com/apache/iotdb-client-go/client"
)

// database option vars
var (
	clientConfig         client.Config
	timeoutInMs          int64 // 0 for no timeout
	usingGroupByApi      bool  // if using group by api when executing query
	singleDatabase       bool  // if using single database, e.g. only one database: root.db. root.db.cpu, root.db.mem belongs to this databse
	useAlignedTimeseries bool  // using aligned timeseries if set true.
	sessionPoolSize      int
)

// Global vars:
var (
	runner      *query.BenchmarkRunner
	sessionPool client.SessionPool
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
	pflag.Bool("aligned-timeseries", false, "Whether to use aligned time series")
	pflag.Uint("session-pool-size", 0, "Session pool size")

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
	user := "root"
	password := "root"
	workers := viper.GetUint("workers")
	usingGroupByApi = viper.GetBool("use-groupby")
	singleDatabase = viper.GetBool("single-database")
	useAlignedTimeseries = viper.GetBool("aligned-timeseries")
	sessionPoolSize = viper.GetInt("session-pool-size")
	timeoutInMs = 0

	log.Printf("tsbs_run_queries_iotdb target: %s:%s. Loading with %d workers. session-pool-size: %d\n", host, port, workers, sessionPoolSize)
	if workers < 5 {
		log.Println("Insertion throughput is strongly related to the number of threads. Use more workers for better performance.")
	}

	clientConfig = client.Config{
		Host:     host,
		Port:     port,
		UserName: user,
		Password: password,
	}

	if sessionPoolSize > 0 {
		poolConfig := &client.PoolConfig{
			Host:     host,
			Port:     port,
			UserName: user,
			Password: password,
		}
		sessionPool = client.NewSessionPool(poolConfig, sessionPoolSize, 60000, 60000, false)
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
	p.printResponses = runner.DoPrintResponses()

	if sessionPoolSize <= 0 {
		p.session = client.NewSession(&clientConfig)
		if err := p.session.Open(false, int(timeoutInMs)); err != nil {
			errMsg := fmt.Sprintf("query processor init error, session is not open: %v\n", err)
			errMsg = errMsg + fmt.Sprintf("timeout setting: %d ms", timeoutInMs)
			log.Fatal(errMsg)
		}
		if workerNumber == 0 {
			_, err := p.session.ExecuteStatement("flush")
			if err != nil {
				log.Fatal(fmt.Sprintf("flush meets error: %v\n", err))
			}
		}
	} else {
		session, err := sessionPool.GetSession()
		if err != nil {
			log.Fatal(fmt.Sprintf("flush meets error: %v\n", err))
		}
		if workerNumber == 0 {
			_, err = session.ExecuteStatement("flush")
			if err != nil {
				log.Fatal(fmt.Sprintf("flush meets error: %v\n", err))
			}
		}
		sessionPool.PutBack(session)
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

	start := time.Now()
	if startTimeInMills > 0 {
		if usingGroupByApi {
			idx := strings.LastIndex(aggregatePaths[0], ".")
			device := aggregatePaths[0][:idx]
			measurement := aggregatePaths[0][idx+1:]
			splits := strings.Split(aggregatePaths[0], ".")
			db := splits[0] + "." + splits[1]
			var err error
			if sessionPoolSize > 0 {
				session, err := sessionPool.GetSession()
				if err == nil {
					dataSet, err = session.ExecuteGroupByQueryIntervalQuery(&db, device, measurement,
						common.TAggregationType_MAX_VALUE, 1,
						&startTimeInMills, &endTimeInMills, &interval, &timeoutInMs, &useAlignedTimeseries)
				} else {
					log.Printf("Get session meets error.\n")
				}
				sessionPool.PutBack(session)
			} else {
				dataSet, err = p.session.ExecuteGroupByQueryIntervalQuery(&db, device, measurement,
					common.TAggregationType_MAX_VALUE, 1,
					&startTimeInMills, &endTimeInMills, &interval, &timeoutInMs, &useAlignedTimeseries)
			}

			if err != nil {
				fmt.Printf("ExecuteGroupByQueryIntervalQuery meets error, "+
					"db: %s, device: %s, measurement: %s, startTime: %d, endTime: %d\n",
					db, device, measurement, startTimeInMills, endTimeInMills)
				return nil, err
			}

			if err == nil {
				if p.printResponses {
					sql = fmt.Sprintf("select max_value(%s) from %s group by ([%d,%d), 60s)",
						measurement, device, startTimeInMills, endTimeInMills)
					printDataSet(sql, dataSet)
				}
			}
		} else {
			if sessionPoolSize > 0 {
				session, err := sessionPool.GetSession()
				if err == nil {
					dataSet, err = session.ExecuteAggregationQueryWithLegalNodes(aggregatePaths,
						[]common.TAggregationType{common.TAggregationType_MAX_VALUE},
						&startTimeInMills, &endTimeInMills, &interval, &timeoutInMs, &legalNodes)
				} else {
					log.Printf("Get session meets error.\n")
				}
				sessionPool.PutBack(session)
			} else {
				dataSet, err = p.session.ExecuteAggregationQueryWithLegalNodes(aggregatePaths,
					[]common.TAggregationType{common.TAggregationType_MAX_VALUE},
					&startTimeInMills, &endTimeInMills, &interval, &timeoutInMs, &legalNodes)
			}

			if err != nil {
				fmt.Printf("ExecuteAggregationQueryWithLegalNodes meets error, "+
					"aggregatePaths: %s, startTime: %d, endTime: %d\n",
					aggregatePaths, startTimeInMills, endTimeInMills)
				return nil, err
			}

			if p.printResponses {
				sql = fmt.Sprintf("Response for ExecuteAggregationQueryWithLegalNodes, "+
					"aggregatePaths: %s, startTime: %d, endTime: %d\n",
					aggregatePaths, startTimeInMills, endTimeInMills)
				printDataSet(sql, dataSet)
			}
		}
	} else {
		if sessionPoolSize > 0 {
			session, err := sessionPool.GetSession()
			if err == nil {
				dataSet, err = session.ExecuteQueryStatement(sql, &timeoutInMs)
			} else {
				log.Printf("Get session meets error.\n")
			}
			sessionPool.PutBack(session)
		} else {
			dataSet, err = p.session.ExecuteQueryStatement(sql, &timeoutInMs)
		}
	}

	if err != nil {
		log.Printf("An error occurred while executing query SQL: %s\n", iotdbQ.SqlQuery)
		return nil, err
	}

	took := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), took)
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
