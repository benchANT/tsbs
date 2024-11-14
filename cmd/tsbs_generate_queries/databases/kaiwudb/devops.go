package kaiwudb

import (
	"fmt"
	"strings"
	"time"

	"github.com/benchant/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/benchant/tsbs/pkg/query"
)

// TODO: Remove the need for this by continuing to bubble up errors
func panicIfErr(err error) {
	if err != nil {
		panic(err.Error())
	}
}

// Devops produces TimescaleDB-specific queries for all the devops query types.
type Devops struct {
	*BaseGenerator
	*devops.Core
}

// getHostWhereWithHostnames creates WHERE SQL statement for multiple hostnames.
// NOTE 'WHERE' itself is not included, just hostname filter clauses, ready to concatenate to 'WHERE' string
func (d *Devops) getHostWhereWithHostnames(hostnames []string) string {
	var hostnameClauses []string
	for _, s := range hostnames {
		hostnameClauses = append(hostnameClauses, fmt.Sprintf("'%s'", s))
	}
	return fmt.Sprintf("satisfying device.hostname in (%s)", strings.Join(hostnameClauses, ","))
}

// getHostWhereString gets multiple random hostnames and creates a WHERE SQL statement for these hostnames.
func (d *Devops) getHostWhereString(nHosts int) string {
	hostnames, err := d.GetRandomHosts(nHosts)
	panicIfErr(err)
	return d.getHostWhereWithHostnames(hostnames)
}

func (d *Devops) getSelectClausesAggMetrics(agg string, metrics []string) []string {
	selectClauses := make([]string, len(metrics))
	for i, m := range metrics {
		selectClauses[i] = fmt.Sprintf("%s(%s)", agg, m)
	}

	return selectClauses
}

func (d *Devops) GroupByTime(qi query.Query, nHosts, numMetrics int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	if len(selectClauses) < 1 {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}
	hostnames, err := d.GetRandomHosts(nHosts)
	if nil != err {
		panic(fmt.Sprintf("get randam host error %s", err.Error()))
	} else if len(hostnames) < 1 {
		panic(fmt.Sprintf("invalid number of host: got %d", len(hostnames)))
	}

	sql := ""
	if nHosts == 1 {
		sql = fmt.Sprintf(`SELECT time_bucket('60s', k_timestamp) as k_timestamp, %s FROM %s.cpu WHERE hostname = '%s' AND k_timestamp >= '%s' AND k_timestamp < '%s' GROUP BY time_bucket('60s', k_timestamp) ORDER BY time_bucket('60s', k_timestamp)`,
			strings.Join(selectClauses, ", "),
			d.CPUDBName,
			hostnames[0],
			//	int(time.UnixMilli(interval.StartUnixMillis()).UTC().UnixMilli()),
			//	int(time.UnixMilli(interval.EndUnixMillis()).UTC().UnixMilli()))
			parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()),
			parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf(`SELECT time_bucket('60s', k_timestamp) as k_timestamp, %s 
			FROM %s.cpu 
			WHERE hostname IN (%s) AND k_timestamp >= '%s' AND k_timestamp < '%s' 
			GROUP BY time_bucket('60s', k_timestamp)
			ORDER BY time_bucket('60s', k_timestamp)`,
			strings.Join(selectClauses, ", "),
			d.CPUDBName,
			"'"+strings.Join(hostnames, "', '")+"'",
			parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()),
			parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))
	}

	humanLabel := fmt.Sprintf("KaiwuDB %d cpu metric(s), random %4d hosts, random %s by 1m", numMetrics, nHosts, timeRange)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) GroupByOrderByLimit(qi query.Query) {
	interval := d.Interval.MustRandWindow(time.Hour)
	sql := fmt.Sprintf(`SELECT time_bucket('60s', k_timestamp) as k_timestamp, max(usage_user) 
		FROM %s.cpu 
		WHERE k_timestamp < '%s' 
		GROUP BY time_bucket('60s', k_timestamp) 
		ORDER BY time_bucket('60s', k_timestamp) 
		LIMIT 5`,
		d.CPUDBName,
		parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))

	humanLabel := "KaiwuDB max cpu over last 5 min-intervals (random end)"
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.EndString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

// GroupByTimeAndPrimaryTag selects the AVG of numMetrics metrics under 'cpu' per device per hour for a day,
func (d *Devops) GroupByTimeAndPrimaryTag(qi query.Query, numMetrics int) {
	metrics, err := devops.GetCPUMetricsSlice(numMetrics)
	panicIfErr(err)
	interval := d.Interval.MustRandWindow(devops.DoubleGroupByDuration)

	selectClauses := d.getSelectClausesAggMetrics("avg", metrics)
	sql := fmt.Sprintf(`SELECT time_bucket('3600s', k_timestamp) as k_timestamp, hostname, %s 
		FROM %s.cpu 
		WHERE k_timestamp >= '%s' AND k_timestamp < '%s' 
		GROUP BY hostname, time_bucket('3600s', k_timestamp) 
		ORDER BY hostname, time_bucket('3600s', k_timestamp)`,
		strings.Join(selectClauses, ", "),
		d.CPUDBName,
		parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()),
		parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))

	humanLabel := devops.GetDoubleGroupByLabel("KaiwuDB", numMetrics)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) MaxAllCPU(qi query.Query, nHosts int, duration time.Duration) {
	interval := d.Interval.MustRandWindow(duration)

	metrics := devops.GetAllCPUMetrics()
	selectClauses := d.getSelectClausesAggMetrics("max", metrics)
	if len(selectClauses) < 1 {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}
	hostnames, err := d.GetRandomHosts(nHosts)
	if nil != err {
		panic(fmt.Sprintf("get randam host error %s", err.Error()))
	} else if len(hostnames) < 1 {
		panic(fmt.Sprintf("invalid number of host: got %d", len(hostnames)))
	}
	sql := ""
	if nHosts == 1 {
		sql = fmt.Sprintf(`SELECT time_bucket('3600s', k_timestamp) as k_timestamp, %s 
		FROM %s.cpu 
		WHERE hostname = '%s' AND k_timestamp >= '%s' AND k_timestamp < '%s' 
		GROUP BY time_bucket('3600s', k_timestamp) 
		ORDER BY time_bucket('3600s', k_timestamp)`,
			strings.Join(selectClauses, ", "),
			d.CPUDBName,
			hostnames[0],
			parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()),
			parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf(`SELECT time_bucket('3600s', k_timestamp) as k_timestamp, %s 
		FROM %s.cpu 
		WHERE hostname IN (%s) AND k_timestamp >= '%s' AND k_timestamp < '%s' 
		GROUP BY time_bucket('3600s', k_timestamp) 
		ORDER BY time_bucket('3600s', k_timestamp)`,
			strings.Join(selectClauses, ", "),
			d.CPUDBName,
			"'"+strings.Join(hostnames, "', '")+"'",
			parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()),
			parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))
	}
	humanLabel := devops.GetMaxAllLabel("KaiwuDB", nHosts)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) LastPointPerHost(qi query.Query) {
	metrics, err := devops.GetCPUMetricsSlice(devops.GetCPUMetricsLen())
	panicIfErr(err)
	selectClauses := d.getSelectClausesAggMetrics("last", metrics)
	if len(selectClauses) != devops.GetCPUMetricsLen() {
		panic(fmt.Sprintf("invalid number of select clauses: got %d", len(selectClauses)))
	}

	sql := fmt.Sprintf(`SELECT last(k_timestamp), %s, hostname FROM %s.cpu GROUP BY hostname`,
		strings.Join(selectClauses, ", "),
		d.CPUDBName)

	humanLabel := "KaiwuDB last row per host"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}

func (d *Devops) HighCPUForHosts(qi query.Query, nHosts int) {
	interval := d.Interval.MustRandWindow(devops.HighCPUDuration)
	//var hostWhereClause string
	//if nHosts == 0 {
	//	hostWhereClause = ""
	//} else {
	//	hostWhereClause = fmt.Sprintf("AND %s", d.getHostWhereString(nHosts))
	//}

	sql := ""
	if nHosts == 1 {
		hostnames, err := d.GetRandomHosts(nHosts)
		panicIfErr(err)
		sql = fmt.Sprintf(`SELECT * FROM %s.cpu WHERE hostname='%s' AND usage_user > 90.0 AND k_timestamp >= '%s' AND k_timestamp < '%s'`,
			d.CPUDBName,
			hostnames[0],
			parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()),
			parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))
	} else {
		sql = fmt.Sprintf(`SELECT * FROM %s.cpu WHERE usage_user > 90.0 AND k_timestamp >= '%s' AND k_timestamp < '%s'`,
			d.CPUDBName,
			parseTime(time.UnixMilli(interval.StartUnixMillis()).UTC()),
			parseTime(time.UnixMilli(interval.EndUnixMillis()).UTC()))
	}
	humanLabel, err := devops.GetHighCPULabel("KaiwuDB", nHosts)
	panicIfErr(err)
	humanDesc := fmt.Sprintf("%s: %s", humanLabel, interval.StartString())
	d.fillInQuery(qi, humanLabel, humanDesc, devops.TableName, sql)
}
