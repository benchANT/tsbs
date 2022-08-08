// tsbs_run_queries_victoriametrics speed tests VictoriaMetrics using requests from stdin or file.
//
// It reads encoded Query objects from stdin, and makes concurrent requests
// to the provided HTTP endpoint. This program has no knowledge of the
// internals of the endpoint.
package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/benchant/tsbs/internal/utils"
	"github.com/benchant/tsbs/pkg/query"
)

// Program option vars:
var (
	vmURLs []string
)

// Global vars:
var (
	runner *query.BenchmarkRunner
)

// Parse args:
func init() {
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)

	pflag.String("urls", "http://localhost:8428",
		"Comma-separated list of VictoriaMetrics ingestion URLs(single-node or VMSelect)")

	pflag.Parse()

	if err := utils.SetupConfigFile(); err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	urls := viper.GetString("urls")
	if len(urls) == 0 {
		log.Fatalf("missing `urls` flag")
	}
	vmURLs = strings.Split(urls, ",")
	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.HTTPPool, newProcessor)
}

type processor struct {
//	url string
//	prettyPrintResponses bool	
	w    *HTTPClient
	opts *HTTPClientDoOptions
}

func newProcessor() query.Processor { return &processor{} }
// query.Processor interface implementation
func (p *processor) Init(workerNum int) {
	// p.url = vmURLs[workerNum%len(vmURLs)]
	// p.prettyPrintResponses = runner.DoPrintResponses()
	p.opts = &HTTPClientDoOptions{
//		Debug:                runner.DebugLevel(),
		PrettyPrintResponses: runner.DoPrintResponses(),
//		chunkSize:            chunkSize,
//		database:             runner.DatabaseName(),
	}
	url := vmURLs[workerNum%len(vmURLs)]
	p.w = NewHTTPClient(url)
}

// query.Processor interface implementation
func (p *processor) ProcessQuery(q query.Query, _ bool) ([]*query.Stat, error) {
	hq := q.(*query.HTTP)
	lag, err := p.w.Do(hq, p.opts)
	if err != nil {
		return nil, err
	}
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), lag)
	return []*query.Stat{stat}, nil
}
