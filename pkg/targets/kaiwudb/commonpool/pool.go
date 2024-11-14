package commonpool

import (
	"context"
	"fmt"
	"sync"

	"github.com/benchant/tsbs/pkg/targets/kaiwudb/thread"
	pgx "github.com/jackc/pgx/v5"
	"github.com/silenceper/pool"
)

type ConnectorPool struct {
	host     string
	user     string
	password string
	port     int
	format   int
	pool     pool.Pool
}

func NewConnectorPool(user string, password string, host string, port int, format int) (*ConnectorPool, error) {
	a := &ConnectorPool{user: user, password: password, host: host, port: port, format: format}
	poolConfig := &pool.Config{
		InitialCap:  1,
		MaxCap:      10000,
		MaxIdle:     10000,
		Factory:     a.factory,
		Close:       a.close,
		IdleTimeout: -1,
	}
	p, err := pool.NewChannelPool(poolConfig)
	if err != nil {
		return nil, err
	}
	a.pool = p
	return a, nil
}

func (a *ConnectorPool) factory() (interface{}, error) {
	thread.Lock()
	defer thread.Unlock()
	url := fmt.Sprintf("dbname=defaultdb host=%s port=%d user=%s password=%s  pg_format=%d sslmode=disable default_query_exec_mode=simple_protocol standard_conforming_strings=on client_encoding=UTF8", a.host, a.port, a.user, a.password, a.format)

	return pgx.Connect(context.Background(), url)
}

func (a *ConnectorPool) close(v interface{}) error {
	if v != nil {
		thread.Lock()
		defer thread.Unlock()
		conn := v.(*pgx.Conn)
		a.Close(conn)
	}
	return nil
}

func (a *ConnectorPool) Get() (*pgx.Conn, error) {
	v, err := a.pool.Get()
	if err != nil {
		return nil, err
	}
	return v.(*pgx.Conn), nil
}

func (a *ConnectorPool) Put(c *pgx.Conn) error {
	return a.pool.Put(c)
}

func (a *ConnectorPool) Close(c *pgx.Conn) error {
	return a.pool.Close(c)
}

func (a *ConnectorPool) Release() {
	a.pool.Release()
}

func (a *ConnectorPool) verifyPassword(password string) bool {
	return password == a.password
}

var connectionMap = sync.Map{}

type Conn struct {
	Connection *pgx.Conn
	pool       *ConnectorPool
}

func (c *Conn) Put() error {
	return c.pool.Put(c.Connection)
}

func GetConnection(user string, password string, host string, port int, format int) (*Conn, error) {
	p, exist := connectionMap.Load(user)
	if exist {
		connectionPool := p.(*ConnectorPool)
		if !connectionPool.verifyPassword(password) {
			newPool, err := NewConnectorPool(user, password, host, port, format)
			if err != nil {
				return nil, err
			}
			connectionPool.Release()
			connectionMap.Store(user, newPool)
			c, err := newPool.Get()
			if err != nil {
				return nil, err
			}
			return &Conn{
				Connection: c,
				pool:       newPool,
			}, nil
		} else {
			c, err := connectionPool.Get()
			if err != nil {
				return nil, err
			}
			if c == nil {
				newPool, err := NewConnectorPool(user, password, host, port, format)
				if err != nil {
					return nil, err
				}
				connectionMap.Store(user, newPool)
				c, err = newPool.Get()
				if err != nil {
					return nil, err
				}
			}
			return &Conn{
				Connection: c,
				pool:       connectionPool,
			}, nil
		}
	} else {
		newPool, err := NewConnectorPool(user, password, host, port, format)
		if err != nil {
			return nil, err
		}
		connectionMap.Store(user, newPool)
		c, err := newPool.Get()
		if err != nil {
			return nil, err
		}
		return &Conn{
			Connection: c,
			pool:       newPool,
		}, nil
	}
}
