package db

import (
	"fmt"
	"runtime"
	"time"

	"github.com/zxfonline/golog"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
)

// RedisNode 一个redis节点
type RedisNode struct {
	address  string
	password string
	pool     *redis.Pool
}

var (
	logger = golog.New("redis")
)

// RedisConn 重新定义
type RedisConn redis.Conn

// NewRedisNode 创建一个节点
func NewRedisNode(addr string, pwd string, dbindex int32, dbIdle int) *RedisNode {
	if dbIdle <= 0 {
		dbIdle = runtime.NumCPU()
	}
	return &RedisNode{
		pool: &redis.Pool{
			MaxIdle:     dbIdle,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				r, err := redis.Dial("tcp", addr)
				if err != nil {
					logger.Errorf("redis connect error:%v", err)
					return nil, err
				}
				if len(pwd) > 0 {
					if _, err := r.Do("auth", pwd); err != nil {
						logger.Errorf("redis auth error :%v", err)
					}
				}
				if dbindex > 0 {
					if _, err := r.Do("select", dbindex); err != nil {
						logger.Errorf("redis select error :%v ", err)
						return nil, err
					}
				}
				return r, nil

			},
		},
		address:  addr,
		password: pwd,
	}
}

// close redis pool
func (node *RedisNode) Close() error {
	return node.pool.Close()
}

// GetRedis  get a connection
func (node *RedisNode) GetRedis() RedisConn {
	return node.pool.Get()
}

// Put 放回去
func (node *RedisNode) Put(conn RedisConn) {
	conn.Close()
}

func GetHashRedis(conn RedisConn, hkey string, key string, valuePtr interface{}) (error, bool) {
	ret, err := conn.Do("hget", hkey, key)
	if err != nil {
		return err, false
	}
	if ret != nil {
		var formaterr error
		switch out := valuePtr.(type) {
		case *string:
			*out, formaterr = redis.String(ret, err)
		case *int:
			*out, formaterr = redis.Int(ret, err)
		case *int32:
			var ret32 int
			ret32, formaterr = redis.Int(ret, err)
			*out = int32(ret32)
		case *int64:
			*out, formaterr = redis.Int64(ret, err)
		case *uint32:
			var retu32 int64
			retu32, formaterr = redis.Int64(ret, err)
			*out = uint32(retu32)
		case *uint64:
			*out, formaterr = redis.Uint64(ret, err)
		case *float64:
			*out, formaterr = redis.Float64(ret, err)
		case *float32:
			var ret64 float64
			ret64, formaterr = redis.Float64(ret, err)
			*out = float32(ret64)
		case *[]byte:
			*out, formaterr = redis.Bytes(ret, err)
		case *bool:
			*out, formaterr = redis.Bool(ret, err)
		case proto.Message:
			var bytes []byte
			bytes, formaterr = redis.Bytes(ret, err)
			if formaterr == nil {
				proto.Unmarshal(bytes, out)
			}
		default:
			formaterr = fmt.Errorf("can't support type:%v", out)
		}
		if formaterr != nil {
			return fmt.Errorf("type error when db.get,err:%s", formaterr.Error()), false
		} else {
			return nil, true
		}
	}
	//数据库中没有该值，返回nil，而不是错误
	return nil, false
}
