package kaiwudb

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/benchant/tsbs/pkg/data"
)

type Serializer struct {
	tmpBuf     *bytes.Buffer
	tableMap   map[string]map[string]struct{}
	superTable map[string]*Table
}

var nothing = struct{}{}

type Table struct {
	columns    map[string]struct{}
	tags       map[string]struct{}
	columnsStr string
	tagsStr    string
}

func FastFormat(buf *bytes.Buffer, v interface{}) string {
	switch v.(type) {
	case int:
		buf.WriteString(strconv.Itoa(v.(int)))
		return "bigint"
	case int64:
		buf.WriteString(strconv.FormatInt(v.(int64), 10))
		return "bigint"
	case float64:
		buf.WriteString(strconv.FormatFloat(v.(float64), 'f', -1, 64))
		return "float"
	case float32:
		buf.WriteString(strconv.FormatFloat(float64(v.(float32)), 'f', -1, 32))
		return "float"
	case bool:
		buf.WriteString(strconv.FormatBool(v.(bool)))
		return "bool"
	case []byte:
		buf.WriteByte('\'')
		buf.WriteString(string(v.([]byte)))
		buf.WriteByte('\'')
		return "char(30)"
	case string:
		buf.WriteByte('\'')
		buf.WriteString(v.(string))
		buf.WriteByte('\'')
		return "char(30)"
	case nil:
		buf.WriteString("null")
		return "null"
	default:
		panic(fmt.Sprintf("unknown field type for %#v", v))
	}
}

var tmpMD5 = map[string]string{}
var tmpIndex = 0

func calculateTable(src []byte) string {
	key := string(src)
	v, exist := tmpMD5[key]
	if exist {
		return v
	}
	tmpIndex += 1
	v = fmt.Sprintf("t_%d", tmpIndex)
	tmpMD5[key] = v
	return v
}

const (
	InsertMetric       = '1'
	CreateTable        = '2'
	InsertMetricAndTag = '3'
	InsertTag          = '4'
	NotNull            = " not null"
)

type tbNameRule struct {
	tag      string
	prefix   string
	nilValue string
}

var tbRuleMap = map[string]*tbNameRule{
	"cpu": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"diskio": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"disk": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"kernel": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"mem": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"net": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"nginx": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"postgresl": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"redis": {
		tag:      "hostname",
		nilValue: "host_null",
	},
	"readings": {
		tag:      "name",
		nilValue: "truck_null",
	},
	"diagnostics": {
		tag:      "name",
		nilValue: "truck_null",
	},
}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	var fieldKeys []string
	var fieldValues []string
	var fieldTypes []string
	var tagValues []string
	var tagKeys []string
	var tagTypes []string
	tKeys := p.TagKeys()
	tValues := p.TagValues()
	fKeys := p.FieldKeys()
	fValues := p.FieldValues()
	superTable := string(p.MeasurementName())
	for i, value := range fValues {
		fType := FastFormat(s.tmpBuf, value)
		fieldKeys = append(fieldKeys, convertKeywords(string(fKeys[i])))
		fieldTypes = append(fieldTypes, fType)
		fieldValues = append(fieldValues, s.tmpBuf.String())
		s.tmpBuf.Reset()
	}

	rule := tbRuleMap[superTable]
	fixedName := ""
	for i, value := range tValues {
		tType := FastFormat(s.tmpBuf, value)
		tagKeys = append(tagKeys, convertKeywords(string(tKeys[i])))
		tagTypes = append(tagTypes, tType)
		if rule != nil && len(fixedName) == 0 && string(tKeys[i]) == rule.tag {
			str, is := value.(string)
			if is {
				fixedName = str
				tagValues = append(tagValues, s.tmpBuf.String())
			} else {
				fixedName = rule.nilValue
				tagValues = append(tagValues, "'"+rule.nilValue+"'")
			}
		} else {
			tagValues = append(tagValues, s.tmpBuf.String())
		}

		s.tmpBuf.Reset()
	}

	subTable := ""
	if rule != nil {
		if len(fixedName) != 0 {
			if len(rule.prefix) == 0 {
				subTable = fixedName
			} else {
				s.tmpBuf.WriteString(rule.prefix)
				s.tmpBuf.WriteString(fixedName)
				subTable = s.tmpBuf.String()
				s.tmpBuf.Reset()
			}
		} else {
			subTable = rule.nilValue
		}
	} else {
		s.tmpBuf.WriteString(superTable)
		for i, v := range tagValues {
			s.tmpBuf.WriteByte(',')
			s.tmpBuf.WriteString(tagKeys[i])
			s.tmpBuf.WriteByte('=')
			s.tmpBuf.WriteString(v)
		}
		subTable = calculateTable(s.tmpBuf.Bytes())
		s.tmpBuf.Reset()
	}

	_, exist := s.superTable[superTable]
	if !exist {
		for i := 0; i < len(fieldTypes); i++ {
			s.tmpBuf.WriteByte(',')
			s.tmpBuf.WriteString(fieldKeys[i])
			s.tmpBuf.WriteByte(' ')
			s.tmpBuf.WriteString(fieldTypes[i])
			s.tmpBuf.WriteString(NotNull)
		}
		columnsStr := s.tmpBuf.String()
		s.tmpBuf.Reset()
		for i := 0; i < len(tagTypes); i++ {
			s.tmpBuf.WriteString(tagKeys[i])
			s.tmpBuf.WriteByte(' ')
			s.tmpBuf.WriteString(tagTypes[i])
			if i == 0 {
				s.tmpBuf.WriteString(" not null")
			}
			if i != len(tagTypes)-1 {
				s.tmpBuf.WriteByte(',')
			}
		}
		tagsStr := s.tmpBuf.String()
		columnsForInsertMetrics := "(k_timestamp:" + strings.Join(fieldKeys, ":") + ":" + rule.tag + ")"
		fmt.Fprintf(w, "%c,%s,%s,create table %s (k_timestamp timestamp not null%s) tags (%s) primary tags (%s)\n",
			CreateTable, superTable, columnsForInsertMetrics, superTable, columnsStr, tagsStr, rule.tag)
		s.tmpBuf.Reset()
		table := &Table{
			columns:    map[string]struct{}{},
			tags:       map[string]struct{}{},
			columnsStr: columnsStr,
			tagsStr:    tagsStr,
		}
		for _, key := range fieldKeys {
			table.columns[key] = nothing
		}
		for _, key := range tagKeys {
			table.tags[key] = nothing
		}
		s.superTable[superTable] = table
		s.tableMap[superTable] = map[string]struct{}{}
	}
	_, exist = s.tableMap[superTable][subTable]
	if !exist {
		fmt.Fprintf(w, "%c,%s,%s,(%d,%s,%s)\n", InsertMetricAndTag, superTable, subTable, p.TimestampInUnixMs(), strings.Join(fieldValues, ","), strings.Join(tagValues, ","))
		s.tableMap[superTable][subTable] = nothing
	} else {
		fmt.Fprintf(w, "%c,%s,%s,%d,(%d,%s,%s)\n", InsertMetric, superTable, subTable, len(fieldValues), p.TimestampInUnixMs(), strings.Join(fieldValues, ","), tagValues[0])
	}

	return nil
}

var keyWords = map[string]bool{}

func convertKeywords(s string) string {
	if is := keyWords[s]; is {
		return fmt.Sprintf("`%s`", s)
	}
	return s
}

func trimString(s string, cutset uint8) string {
	result := ""
	for i := 0; i < len(s); i++ {
		if s[i] != cutset {
			result = fmt.Sprintf("%s%c", result, s[i])
		}
	}
	return result
}

func trimColumnName(s string) string {
	columnNameAndTypes := strings.Split(s, ",")
	columnResult := make([]string, 0)
	for _, columnNameAndType := range columnNameAndTypes {
		columnName := strings.Split(columnNameAndType, " ")
		if len(columnName[0]) > 20 {
			columnName[0] = columnName[0][:20]
		}
		columnResult = append(columnResult, strings.Join(columnName, " "))
	}
	return strings.Join(columnResult, ",")
}
