package clickhouse

import (
	"github.com/timescale/tsbs/pkg/data"
	"hash/fnv"
	"strings"
)

// hostnameIndexer is used to consistently send the same hostnames to the same queue
type hostnameIndexer struct {
	partitions uint
}

// scan.PointIndexer interface implementation
func (i *hostnameIndexer) GetIndex(item *data.LoadedPoint) int {
	p := item.Data.(*point)
	hostname := strings.SplitN(p.row.tags, ",", 2)[0]
	h := fnv.New32a()
	h.Write([]byte(hostname))
	return int(h.Sum32()) % int(i.partitions)
}
