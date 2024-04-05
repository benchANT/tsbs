package influx2

import (
	"github.com/benchant/tsbs/pkg/data/serialize"
	"github.com/benchant/tsbs/pkg/data/source"
	"github.com/benchant/tsbs/pkg/targets"
	"github.com/benchant/tsbs/pkg/targets/constants"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"time"
)

func NewTarget() targets.ImplementedTarget {
	return &influxTarget{}
}

type influxTarget struct {
}

func (t *influxTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"urls", "http://localhost:8086", "InfluxDB URLs, comma-separated. Will be used in a round-robin fashion.")
	flagSet.Int(flagPrefix+"replication-factor", 1, "Cluster replication factor (only applies to clustered databases).")
	flagSet.String(flagPrefix+"consistency", "all", "Write consistency. Must be one of: any, one, quorum, all.")
	flagSet.Duration(flagPrefix+"backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flagSet.Bool(flagPrefix+"gzip", true, "Whether to gzip encode requests (default true).")
	flagSet.String(flagPrefix+"org-id", "tsbs", "InfluxDB organization ID (default tsbs).")
	flagSet.String(flagPrefix+"auth-token", "tsbs-token", "InfluxDB Authorization Token (default tsbs-token).")
}

func (t *influxTarget) TargetName() string {
	return constants.FormatInflux
}

func (t *influxTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *influxTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("not implemented")
}
