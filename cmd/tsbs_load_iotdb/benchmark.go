package main

import (
	"bufio"
	"bytes"
	"github.com/apache/iotdb-client-go/client"
	"github.com/spaolacci/murmur3"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
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

// GetPointIndexer only used for 100 workers
//func (b *iotdbBenchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
//	if maxPartitions > 1 {
//		m := map[string][]int{
//			"cpu":       {0, 10},
//			"diskio":    {11, 21},
//			"disk":      {22, 32},
//			"kernel":    {33, 43},
//			"mem":       {44, 54},
//			"net":       {55, 65},
//			"nginx":     {66, 76},
//			"postgresl": {77, 87},
//			"redis":     {88, 99},
//		}
//		return &iotdbIndexer{buffer: &bytes.Buffer{}, maxPartitions: maxPartitions,
//			intervalMap: m, cache: map[string]uint{}}
//	}
//	return &targets.ConstantIndexer{}
//}

//func (i *iotdbIndexer) GetIndex(item data.LoadedPoint) uint {
//	p := item.Data.(*iotdbPoint)
//	idx, ok := i.cache[p.deviceID]
//	if ok {
//		return idx
//	}
//
//	interval := i.intervalMap[p.db]
//	tmp := 0
//	for _, s := range p.deviceID {
//		if s == '0' || s == '1' || s == '2' || s == '3' || s == '4' || s == '5' || s == '6' || s == '7' || s == '8' || s == '9' {
//			tmp = tmp*10 + int(s-'0')
//		}
//	}
//	ret := interval[0] + (tmp % 11)
//	i.cache[p.deviceID] = uint(ret)
//	return uint(ret)
//}
