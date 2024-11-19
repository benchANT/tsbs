package kaiwudb

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/benchant/tsbs/cmd/tsbs_generate_queries/uses/devops"
)

const layout = "2006-01-02 15:04:05.000"

type ParsePrepare struct {
	typ string
}

func NewParsePrepare(label string) *ParsePrepare {
	dbName := "KaiwuDB"
	var typ string
	switch label {
	case fmt.Sprintf("KaiwuDB %d cpu metric(s), random %4d hosts, random %s by 1m", 1, 1, "1h0m0s"):
		typ = devops.LabelSingleGroupby + "-1-1-1"
	case fmt.Sprintf("KaiwuDB %d cpu metric(s), random %4d hosts, random %s by 1m", 1, 1, "12h0m0s"):
		typ = devops.LabelSingleGroupby + "-1-1-12"
	case fmt.Sprintf("KaiwuDB %d cpu metric(s), random %4d hosts, random %s by 1m", 1, 8, "1h0m0s"):
		typ = devops.LabelSingleGroupby + "-1-8-1"
	case fmt.Sprintf("KaiwuDB %d cpu metric(s), random %4d hosts, random %s by 1m", 5, 1, "1h0m0s"):
		typ = devops.LabelSingleGroupby + "-5-1-1"
	case fmt.Sprintf("KaiwuDB %d cpu metric(s), random %4d hosts, random %s by 1m", 5, 1, "12h0m0s"):
		typ = devops.LabelSingleGroupby + "-5-1-12"
	case devops.GetDoubleGroupByLabel(dbName, 1):
		typ = devops.LabelDoubleGroupby + "1"
	case devops.GetDoubleGroupByLabel(dbName, 5):
		typ = devops.LabelDoubleGroupby + "5"
	case devops.GetDoubleGroupByLabel(dbName, devops.GetCPUMetricsLen()):
		typ = devops.LabelDoubleGroupby + "all"
	case devops.GetMaxAllLabel(dbName, 1):
		typ = devops.LabelMaxAll + "1"
	case devops.GetMaxAllLabel(dbName, 8):
		typ = devops.LabelMaxAll + "8"
	case "KaiwuDB max cpu over last 5 min-intervals (random end)":
		typ = devops.LabelGroupbyOrderbyLimit
	case getHighCPULabel(dbName, 1):
		typ = devops.LabelHighCPU + "-1"
	case getHighCPULabel(dbName, 0):
		typ = devops.LabelHighCPU + "-all"
	case "KaiwuDB last row per host":
		typ = devops.LabelLastpoint
	default:
		typ = "not-support"
	}

	return &ParsePrepare{typ: typ}
}

func (p *ParsePrepare) Parse(qry string, debug bool) (string, []interface{}) {
	switch p.typ {
	case devops.LabelSingleGroupby + "-1-1-1":
		return HostRangeFast(qry, debug)
	case devops.LabelSingleGroupby + "-5-1-1",
		devops.LabelSingleGroupby + "-1-1-12",
		devops.LabelSingleGroupby + "-5-1-12",
		devops.LabelHighCPU + "-1",
		devops.LabelMaxAll + "1":
		return p.HostRange(qry, debug)
	case devops.LabelSingleGroupby + "-1-8-1",
		devops.LabelSingleGroupby + "-5-8-1",
		devops.LabelMaxAll + "8":
		return p.HostsRange(qry, debug)
	case devops.LabelDoubleGroupby + "-1",
		devops.LabelDoubleGroupby + "-5",
		devops.LabelDoubleGroupby + "-all",
		devops.LabelHighCPU + "-all":
		return p.Range(qry, debug)
	case devops.LabelGroupbyOrderbyLimit:
		return p.Less(qry, debug)
	case devops.LabelLastpoint:
		return p.NotChange(qry, debug)
	default:
		return p.NotChange(qry, debug)
	}
}

func HostRangeFast(qry string, debug bool) (string, []interface{}) {
	arr := strings.Split(qry, " ")

	hostname := strings.Trim(arr[11], "'")
	arr[11] = "$1"

	t := strings.Trim(arr[16], "'")
	if len(t) == 11 {
		t += "0"
	}
	if len(t) == 10 {
		t += "00"
	}
	if len(t) == 8 {
		t += ".000"
	}

	str := fmt.Sprintf("%s %s", strings.Trim(arr[15], "'"), t)
	s, err := time.Parse(layout, str)
	if err != nil {
		panic(err)
	}
	arr[15] = ""
	arr[16] = "$2"

	t = strings.Trim(arr[21], "'")
	if len(t) == 11 {
		t += "0"
	}
	if len(t) == 10 {
		t += "00"
	}
	if len(t) == 8 {
		t += ".000"
	}
	str = fmt.Sprintf("%s %s", strings.Trim(arr[20], "'"), t)
	e, err := time.Parse(layout, str)
	if err != nil {
		panic(err)
	}
	arr[20] = ""
	arr[21] = "$3"

	var b strings.Builder
	b.Grow(len(arr))
	b.WriteString(arr[0])
	for _, ss := range arr[1:] {
		if ss != "" {
			b.WriteString(" ")
		}
		b.WriteString(ss)
	}
	sql := b.String()
	if debug {
		fmt.Println("args:", hostname, s, e)
		fmt.Println("sql:", sql)
	}

	return sql, []interface{}{hostname, s, e}
}

func (p *ParsePrepare) HostRange(qry string, debug bool) (string, []interface{}) {
	var args []interface{}

	host := getHost(qry)
	args = append(args, strings.Trim(host, "'"))
	qry = strings.ReplaceAll(qry, host, fmt.Sprintf("$%d", len(args)))

	start, s := getStart(qry)
	if start != "" {
		args = append(args, s)
		qry = strings.ReplaceAll(qry, start, fmt.Sprintf("$%d", len(args)))
	}

	end, e := getEnd(qry)
	if end != "" {
		args = append(args, e)
		qry = strings.ReplaceAll(qry, end, fmt.Sprintf("$%d", len(args)))
	}

	if debug {
		fmt.Println("sql:", qry, args)
	}

	return qry, args
}

func (p *ParsePrepare) HostsRange(qry string, debug bool) (string, []interface{}) {
	var args []interface{}

	str, hosts := getHosts(qry)
	args = append(args, hosts)
	qry = strings.ReplaceAll(qry, str, fmt.Sprintf("$%d", len(args)))

	start, s := getStart(qry)
	if start != "" {
		args = append(args, s)
		qry = strings.ReplaceAll(qry, start, fmt.Sprintf("$%d", len(args)))
	}

	end, e := getEnd(qry)
	if end != "" {
		args = append(args, e)
		qry = strings.ReplaceAll(qry, end, fmt.Sprintf("$%d", len(args)))
	}

	if debug {
		fmt.Println("sql:", qry, args)
	}
	return qry, args
}

func (p *ParsePrepare) Range(qry string, debug bool) (string, []interface{}) {
	var args []interface{}
	start, s := getStart(qry)
	if start != "" {
		args = append(args, s)
		qry = strings.ReplaceAll(qry, start, fmt.Sprintf("$%d", len(args)+1))
	}

	end, e := getEnd(qry)
	if end != "" {
		args = append(args, e)
		qry = strings.ReplaceAll(qry, end, fmt.Sprintf("$%d", len(args)+1))
	}

	if debug {
		fmt.Println("sql:", qry, args)
	}
	return qry, args
}

func (p *ParsePrepare) Less(qry string, debug bool) (string, []interface{}) {
	var args []interface{}

	end, e := getEnd(qry)
	if end != "" {
		args = append(args, e)
		qry = strings.ReplaceAll(qry, end, fmt.Sprintf("$%d", len(args)+1))
	}

	if debug {
		fmt.Println("sql:", qry, args)
	}

	return qry, args
}

func (p *ParsePrepare) NotChange(qry string, debug bool) (string, []interface{}) {
	var args []interface{}

	return qry, args
}

func getHighCPULabel(dbName string, nHosts int) string {
	humanLabel, _ := devops.GetHighCPULabel(dbName, nHosts)

	return humanLabel
}

func getHost(qry string) string {
	regex := regexp.MustCompile(`hostname = ('host_\d+')`)
	matches := regex.FindStringSubmatch(qry)
	if len(matches) > 0 {
		return matches[1]
	}

	return ""
}

func getHosts(qry string) (string, []string) {
	regex := regexp.MustCompile(`hostname IN (\([^()]*\))`)
	matches := regex.FindStringSubmatch(qry)
	if len(matches) > 0 {
		hosts := strings.Trim(matches[1], "()'")
		return matches[1], strings.Split(hosts, ",")
	}

	return "", []string{}
}

func getStart(qry string) (string, time.Time) {
	regex := regexp.MustCompile(`>= ('[^']*')`)
	matches := regex.FindStringSubmatch(qry)
	if len(matches) > 0 {
		str := strings.Trim(matches[1], "'")
		if len(str) == 22 {
			str += "0"
		}
		if len(str) == 21 {
			str += "00"
		}
		if len(str) == 19 {
			str += ".000"
		}
		s, err := time.Parse(layout, str)
		if err != nil {
			panic(err)
		}

		return matches[1], s
	}

	return "", time.Time{}
}

func getEnd(qry string) (string, time.Time) {
	regex := regexp.MustCompile(`< ('[^']*')`)
	matches := regex.FindStringSubmatch(qry)
	if len(matches) > 0 {
		str := strings.Trim(matches[1], "'")
		if len(str) == 22 {
			str += "0"
		}
		if len(str) == 21 {
			str += "00"
		}
		if len(str) == 19 {
			str += ".000"
		}
		e, err := time.Parse(layout, str)
		if err != nil {
			panic(err)
		}

		return matches[1], e
	}
	return "", time.Time{}
}
