package kaiwudb

import "C"
import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/benchant/tsbs/pkg/targets"
	"github.com/benchant/tsbs/pkg/targets/kaiwudb/commonpool"
)

var fatal = log.Fatalf

type dbCreator struct {
	opts *LoadingOptions
	ds   targets.DataSource
	db   *commonpool.Conn
}

var IOTPRE = []string{"readings", "diagnostics"}
var DEVOPSPRE = []string{"cpu", "diskio", "disk", "kernel", "mem", "net", "nginx", "postgresl", "redis"}

func (d *dbCreator) Init() {
	db, err := commonpool.GetConnection(d.opts.User, d.opts.Pass, d.opts.Host[0], d.opts.Port[0], 0)
	if err != nil {
		panic(fmt.Sprintf("kaiwudb can not get connection %s", err.Error()))
	}
	d.db = db
}

func (d *dbCreator) DBExists(dbName string) bool {
	return true
}

func (d *dbCreator) CreateDB(dbName string) error {
	ctx := context.Background()
	// 创建时序数据库
	sql := fmt.Sprintf("create schema %s ", dbName)
	_, err := d.db.Connection.Exec(ctx, sql)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		panic(fmt.Sprintf("kaiwudb create schema failed, err :%s", err))
	}

	return nil
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	ctx := context.Background()

	sql := fmt.Sprintf("drop schema %s CASCADE", dbName)
	_, err := d.db.Connection.Exec(ctx, sql)
	if err != nil && !strings.Contains(err.Error(), "does not exist") {
		panic(fmt.Sprintf("kaiwudb drop schema failed, err :%s", err))
	}
	return nil
}

func (d *dbCreator) Close() {
	if d.db != nil {
		d.db.Put()
	}
}
