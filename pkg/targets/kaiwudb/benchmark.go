package kaiwudb

import (
	"github.com/benchant/tsbs/pkg/data/source"
	"github.com/benchant/tsbs/pkg/targets"
)

var (
	KAIWUINSERT     = "insert"
	KAIWUPREPARE    = "prepare"
	KAIWUPREPAREIOT = "prepareiot"
)

func NewBenchmark(dbName string, opts *LoadingOptions, dataSourceConfig *source.DataSourceConfig) (targets.Benchmark, error) {
	var ds targets.DataSource
	if dataSourceConfig.Type == source.FileDataSourceType {
		ds = newFileDataSource(dataSourceConfig.File.Location)
	} else {
		panic("not implement")
	}

	return &benchmark{
		opts:   opts,
		ds:     ds,
		dbName: dbName,
	}, nil
}

type benchmark struct {
	opts   *LoadingOptions
	ds     targets.DataSource
	dbName string
}

func (b *benchmark) GetDataSource() targets.DataSource {
	return b.ds
}

func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if maxPartitions > 1 {

		return &indexer{partitions: maxPartitions, tmp: map[string]uint{}, index: 0, Buffer: b.opts.Buffer}
	}
	return &targets.ConstantIndexer{}
}

func (b *benchmark) GetProcessor() targets.Processor {
	switch b.opts.Type {
	case KAIWUINSERT:
		return newProcessorInsert(b.opts, b.dbName)
	case KAIWUPREPARE:
		return newProcessorPrepare(b.opts, b.dbName)
	default:
		return nil
	}
}

func (b *benchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{opts: b.opts, ds: b.ds}
}
