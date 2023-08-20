package iotdb

import (
	"github.com/apache/iotdb-client-go/client"
)

var (
	GlobalTabletSchemaMap = make(map[string][]*client.MeasurementSchema)
	GlobalMeasurementMap  = make(map[string][]string)
	GlobalDataTypeMap     = make(map[string][]client.TSDataType)

	cpuFields = []string{
		"usage_user",
		"usage_system",
		"usage_idle",
		"usage_nice",
		"usage_iowait",
		"usage_irq",
		"usage_softirq",
		"usage_steal",
		"usage_guest",
		"usage_guest_nice",
	}

	diskioFields = []string{
		"reads",
		"writes",
		"read_bytes",
		"write_bytes",
		"read_time",
		"write_time",
		"io_time",
	}

	diskFields = []string{
		"total",
		"free",
		"usage",
		"used_percent",
		"inodes_total",
		"inodes_free",
		"inodes_used",
	}

	kernelFields = []string{
		"boot_time",
		"interrupts",
		"context_switches",
		"processes_forked",
		"disk_pages_in",
		"disk_pages_out",
	}

	memFields = []string{
		"total",
		"available",
		"usage",
		"free",
		"cached",
		"buffered",
		"used_percent",
		"available_percent",
		"buffered_percent",
	}

	netFields = []string{
		"bytes_sent",
		"bytes_recv",
		"packets_sent",
		"packets_recv",
		"err_in",
		"err_out",
		"drop_in",
		"drop_out",
	}

	nginxFields = []string{
		"accepts",
		"active",
		"handled",
		"reading",
		"requests",
		"waiting",
		"writing",
	}

	postgreslFields = []string{
		"numbackends",
		"xact_commit",
		"xact_rollback",
		"blks_read",
		"blks_hit",
		"tup_returned",
		"tup_fetched",
		"tup_inserted",
		"tup_updated",
		"tup_deleted",
		"conflicts",
		"temp_files",
		"temp_bytes",
		"deadlocks",
		"blk_read_time",
		"blk_write_time",
	}

	redisFields = []string{
		"uptime_in_seconds",
		"total_connections_received",
		"expired_keys",
		"evicted_keys",
		"keyspace_hits",
		"keyspace_misses",
		"instantaneous_ops_per_sec",
		"instantaneous_input_kbps",
		"instantaneous_output_kbps",
		"connected_clients",
		"used_memory",
		"used_memory_rss",
		"used_memory_peak",
		"used_memory_lua",
		"rdb_changes_since_last_save",
		"sync_full",
		"sync_partial_ok",
		"sync_partial_err",
		"pubsub_channels",
		"pubsub_patterns",
		"latest_fork_usec",
		"connected_slaves",
		"master_repl_offset",
		"repl_backlog_active",
		"repl_backlog_size",
		"repl_backlog_histlen",
		"mem_fragmentation_ratio",
		"used_cpu_sys",
		"used_cpu_user",
		"used_cpu_sys_children",
		"used_cpu_user_children",
	}
)

func init() {
	for _, f := range cpuFields {
		GlobalMeasurementMap["cpu"] = append(GlobalMeasurementMap["cpu"], f)
		GlobalDataTypeMap["cpu"] = append(GlobalDataTypeMap["cpu"], client.INT32)
		GlobalTabletSchemaMap["cpu"] = append(GlobalTabletSchemaMap["cpu"], &client.MeasurementSchema{Measurement: f, DataType: client.INT32})
	}
	for _, f := range diskioFields {
		GlobalMeasurementMap["diskio"] = append(GlobalMeasurementMap["diskio"], f)
		GlobalDataTypeMap["diskio"] = append(GlobalDataTypeMap["diskio"], client.INT32)
		GlobalTabletSchemaMap["diskio"] = append(GlobalTabletSchemaMap["diskio"], &client.MeasurementSchema{Measurement: f, DataType: client.INT32})
	}
	for _, f := range diskFields {
		GlobalMeasurementMap["disk"] = append(GlobalMeasurementMap["disk"], f)
		GlobalDataTypeMap["disk"] = append(GlobalDataTypeMap["disk"], client.INT64)
		GlobalTabletSchemaMap["disk"] = append(GlobalTabletSchemaMap["disk"], &client.MeasurementSchema{Measurement: f, DataType: client.INT64})
	}
	for _, f := range kernelFields {
		GlobalMeasurementMap["kernel"] = append(GlobalMeasurementMap["kernel"], f)
		GlobalDataTypeMap["kernel"] = append(GlobalDataTypeMap["kernel"], client.INT32)
		GlobalTabletSchemaMap["kernel"] = append(GlobalTabletSchemaMap["kernel"], &client.MeasurementSchema{Measurement: f, DataType: client.INT32})
	}
	for _, f := range memFields {
		GlobalMeasurementMap["mem"] = append(GlobalMeasurementMap["mem"], f)

		if f == "used_percent" || f == "available_percent" || f == "buffered_percent" {
			GlobalDataTypeMap["mem"] = append(GlobalDataTypeMap["mem"], client.DOUBLE)
			GlobalTabletSchemaMap["mem"] = append(GlobalTabletSchemaMap["mem"], &client.MeasurementSchema{Measurement: f, DataType: client.DOUBLE})
		} else {
			GlobalDataTypeMap["mem"] = append(GlobalDataTypeMap["mem"], client.INT64)
			GlobalTabletSchemaMap["mem"] = append(GlobalTabletSchemaMap["mem"], &client.MeasurementSchema{Measurement: f, DataType: client.INT64})
		}
	}
	for _, f := range netFields {
		GlobalMeasurementMap["net"] = append(GlobalMeasurementMap["net"], f)
		GlobalDataTypeMap["net"] = append(GlobalDataTypeMap["net"], client.INT32)
		GlobalTabletSchemaMap["net"] = append(GlobalTabletSchemaMap["net"], &client.MeasurementSchema{Measurement: f, DataType: client.INT32})
	}
	for _, f := range nginxFields {
		GlobalMeasurementMap["nginx"] = append(GlobalMeasurementMap["nginx"], f)
		GlobalDataTypeMap["nginx"] = append(GlobalDataTypeMap["nginx"], client.INT32)
		GlobalTabletSchemaMap["nginx"] = append(GlobalTabletSchemaMap["nginx"], &client.MeasurementSchema{Measurement: f, DataType: client.INT32})
	}
	for _, f := range postgreslFields {
		GlobalMeasurementMap["postgresl"] = append(GlobalMeasurementMap["postgresl"], f)
		GlobalDataTypeMap["postgresl"] = append(GlobalDataTypeMap["postgresl"], client.INT32)
		GlobalTabletSchemaMap["postgresl"] = append(GlobalTabletSchemaMap["postgresl"], &client.MeasurementSchema{Measurement: f, DataType: client.INT32})
	}
	for _, f := range redisFields {
		GlobalMeasurementMap["redis"] = append(GlobalMeasurementMap["redis"], f)
		GlobalDataTypeMap["redis"] = append(GlobalDataTypeMap["redis"], client.INT64)
		GlobalTabletSchemaMap["redis"] = append(GlobalTabletSchemaMap["redis"], &client.MeasurementSchema{Measurement: f, DataType: client.INT64})
	}
}
