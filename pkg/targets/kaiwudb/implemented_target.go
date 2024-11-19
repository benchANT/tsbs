package kaiwudb

import (
	"bytes"

	"github.com/benchant/tsbs/pkg/data/serialize"
	"github.com/benchant/tsbs/pkg/data/source"
	"github.com/benchant/tsbs/pkg/targets"
	"github.com/benchant/tsbs/pkg/targets/constants"
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
)

func NewTarget() targets.ImplementedTarget {
	return &kaiwudbTarget{}
}

type kaiwudbTarget struct {
}

func (t *kaiwudbTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"user", "root", "User to connect to KaiwuDB")
	flagSet.String(flagPrefix+"pass", "", "Password for user connecting to KaiwuDB")
	flagSet.StringSlice(flagPrefix+"host", []string{"", "", ""}, "KaiwuDB host")
	flagSet.IntSlice(flagPrefix+"port", []int{26257, 26258, 26259}, "KaiwuDB client Port")
	flagSet.String(flagPrefix+"insert-type", "prepare", "KaiwuDB insert type")
}

func (t *kaiwudbTarget) TargetName() string {
	return constants.FormatKaiwuDB
}

func (t *kaiwudbTarget) Serializer() serialize.PointSerializer {
	return &Serializer{
		tableMap:   map[string]map[string]struct{}{},
		superTable: map[string]*Table{},
		tmpBuf:     &bytes.Buffer{},
	}
}

func (t *kaiwudbTarget) Benchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper,
) (targets.Benchmark, error) {
	var loadingOptions LoadingOptions
	if err := v.Unmarshal(&loadingOptions); err != nil {
		return nil, err
	}
	return NewBenchmark(targetDB, &loadingOptions, dataSourceConfig)
}
