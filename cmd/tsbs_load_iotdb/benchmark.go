package main

import (
	"bufio"
	"bytes"
	"github.com/apache/iotdb-client-go/client"
	"github.com/benchant/tsbs/load"
	"github.com/benchant/tsbs/pkg/data"
	"github.com/benchant/tsbs/pkg/targets"
	"github.com/spaolacci/murmur3"
	"math"
)

func newBenchmark(clientConfig client.Config, loaderConfig load.BenchmarkRunnerConfig) targets.Benchmark {
	return &iotdbBenchmark{
		clientConfig:   clientConfig,
		loaderConfig:   loaderConfig,
		recordsMaxRows: recordsMaxRows,
		tabletSize:     tabletSize,
	}
}

type iotdbBenchmark struct {
	clientConfig   client.Config
	loaderConfig   load.BenchmarkRunnerConfig
	recordsMaxRows int
	tabletSize     int
}

type iotdbIndexer struct {
	buffer        *bytes.Buffer
	maxPartitions uint
	hashEndGroups []uint32
	intervalMap   map[string][]int
	cache         map[string]uint
}

func (b *iotdbBenchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	if maxPartitions > 1 {
		interval := uint32(math.MaxUint32 / maxPartitions)
		hashEndGroups := make([]uint32, maxPartitions)
		for i := 0; i < int(maxPartitions); i++ {
			if i == int(maxPartitions)-1 {
				hashEndGroups[i] = math.MaxUint32
			} else {
				hashEndGroups[i] = interval*uint32(i+1) - 1
			}
		}
		return &iotdbIndexer{buffer: &bytes.Buffer{}, hashEndGroups: hashEndGroups,
			maxPartitions: maxPartitions, cache: map[string]uint{}}
	}
	return &targets.ConstantIndexer{}
}

func (i *iotdbIndexer) GetIndex(item data.LoadedPoint) uint {
	p := item.Data.(*iotdbPoint)
	idx, ok := i.cache[p.deviceID]
	if ok {
		return idx
	}

	i.buffer.WriteString(p.deviceID)
	hash := murmur3.Sum32WithSeed(i.buffer.Bytes(), 0x12345678)
	i.buffer.Reset()
	for j := 0; j < int(i.maxPartitions); j++ {
		if hash <= i.hashEndGroups[j] {
			idx = uint(j)
			break
		}
	}
	i.cache[p.deviceID] = idx
	return idx
}

func (b *iotdbBenchmark) GetDataSource() targets.DataSource {
	return &fileDataSource{scanner: bufio.NewScanner(load.GetBufferedReader(b.loaderConfig.FileName))}
}

func (b *iotdbBenchmark) GetBatchFactory() targets.BatchFactory {
	return &factory{}
}

func (b *iotdbBenchmark) GetProcessor() targets.Processor {
	return &processor{
		recordsMaxRows:       b.recordsMaxRows,
		tabletSize:           tabletSize,
		loadToSCV:            loadToSCV,
		csvFilepathPrefix:    csvFilepathPrefix,
		useAlignedTimeseries: useAlignedTimeseries,
		storeTags:            storeTags,
	}
}

func (b *iotdbBenchmark) GetDBCreator() targets.DBCreator {
	return &dbCreator{
		loadToSCV: loadToSCV,
	}
}
