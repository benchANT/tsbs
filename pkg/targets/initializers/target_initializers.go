package initializers

import (
	"fmt"
	"github.com/benchant/tsbs/pkg/targets"
	"github.com/benchant/tsbs/pkg/targets/akumuli"
	"github.com/benchant/tsbs/pkg/targets/cassandra"
	"github.com/benchant/tsbs/pkg/targets/clickhouse"
	"github.com/benchant/tsbs/pkg/targets/constants"
	"github.com/benchant/tsbs/pkg/targets/crate"
	"github.com/benchant/tsbs/pkg/targets/influx"
	"github.com/benchant/tsbs/pkg/targets/iotdb"
	"github.com/benchant/tsbs/pkg/targets/kaiwudb"
	"github.com/benchant/tsbs/pkg/targets/mongo"
	"github.com/benchant/tsbs/pkg/targets/prometheus"
	"github.com/benchant/tsbs/pkg/targets/questdb"
	"github.com/benchant/tsbs/pkg/targets/siridb"
	"github.com/benchant/tsbs/pkg/targets/timescaledb"
	"github.com/benchant/tsbs/pkg/targets/timestream"
	"github.com/benchant/tsbs/pkg/targets/victoriametrics"
	"strings"
)

func GetTarget(format string) targets.ImplementedTarget {
	switch format {
	case constants.FormatTimescaleDB:
		return timescaledb.NewTarget()
	case constants.FormatAkumuli:
		return akumuli.NewTarget()
	case constants.FormatCassandra:
		return cassandra.NewTarget()
	case constants.FormatClickhouse:
		return clickhouse.NewTarget()
	case constants.FormatCrateDB:
		return crate.NewTarget()
	case constants.FormatInflux:
		return influx.NewTarget()
	case constants.FormatMongo:
		return mongo.NewTarget()
	case constants.FormatPrometheus:
		return prometheus.NewTarget()
	case constants.FormatSiriDB:
		return siridb.NewTarget()
	case constants.FormatVictoriaMetrics:
		return victoriametrics.NewTarget()
	case constants.FormatTimestream:
		return timestream.NewTarget()
	case constants.FormatQuestDB:
		return questdb.NewTarget()
	case constants.FormatIoTDB:
		return iotdb.NewTarget()
	case constants.FormatKaiwuDB:
		return kaiwudb.NewTarget()
	}

	supportedFormatsStr := strings.Join(constants.SupportedFormats(), ",")
	panic(fmt.Sprintf("Unrecognized format %s, supported: %s", format, supportedFormatsStr))
}
