module github.com/benchant/tsbs

go 1.14

replace github.com/mediocregopher/radix/v3 => github.com/filipecosta90/radix/v3 v3.5.1-0.20220627150505-ae9cc9425e77

require (
	github.com/HdrHistogram/hdrhistogram-go v1.0.0
	github.com/RedisTimeSeries/redistimeseries-go v1.4.3
	github.com/SiriDB/go-siridb-connector v0.0.0-20190110105621-86b34c44c921
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883
	github.com/aws/aws-sdk-go v1.35.13
	github.com/blagojts/viper v1.6.3-0.20200313094124-068f44cf5e69
	github.com/cheggaaa/pb/v3 v3.0.6
	github.com/globalsign/mgo v0.0.0-20181015135952-eeefdecb41b8
	github.com/gocql/gocql v0.0.0-20190810123941-df4b9cc33030
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.1
	github.com/google/flatbuffers v1.11.0
	github.com/google/go-cmp v0.5.5
	github.com/jackc/pgx/v4 v4.8.0
	github.com/jmoiron/sqlx v1.2.1-0.20190826204134-d7d95172beb5
	github.com/kshvakov/clickhouse v1.3.11
	github.com/lib/pq v1.3.0
	github.com/mediocregopher/radix/v3 v3.7.0
	github.com/mediocregopher/radix/v4 v4.1.0
	github.com/mitchellh/gox v1.0.1 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/common v0.13.0
	github.com/shirou/gopsutil v3.21.3+incompatible
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/tcnksm/ghr v0.14.0 // indirect
	github.com/timescale/promscale v0.0.0-20201006153045-6a66a36f5c84
	github.com/timescale/tsbs v0.0.0-20220119183622-bcc00137d72d
	github.com/transceptor-technology/go-qpack v0.0.0-20190116123619-49a14b216a45
	github.com/valyala/fasthttp v1.15.1
	go.mongodb.org/mongo-driver v1.4.6
	go.uber.org/atomic v1.6.0
	golang.org/x/net v0.0.0-20210614182718-04defd469f4e
	golang.org/x/time v0.0.0-20200630173020-3af7569d3a1e
	gopkg.in/yaml.v2 v2.3.0
)
