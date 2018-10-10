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
	// return fmt.Errorf("value not found")
	//数据库中没有该值，返回nil，而不是错误
	return nil, false
}

// SetExpiryRedis 设置一个数值，并且设置过期时间,如果只设置一个过期时间value设置为nil
func SetExpiryRedis(conn RedisConn, key string, value interface{}, expiry uint32) {
	if value != nil {
		conn.Send("set", key, value)
	}
	if expiry > 0 {
		conn.Send("expire", key, expiry)
	}
	conn.Flush()
}

// IncrHashWithExpiryRedis 自增一个键值，并设置key的过期时间
func IncrHashWithExpiryRedis(conn RedisConn, hkey string, key string, step int, expiry uint32) {
	conn.Send("hincrby", hkey, key, step)
	if expiry > 0 {
		conn.Send("expire", hkey, expiry)
	}
	conn.Flush()
}

// IncrExpiryRedis 设置一个数值，并且设置过期时间,如果只设置一个过期时间value设置为nil
func IncrExpiryRedis(conn RedisConn, key string, value interface{}, expire uint32) (int64, error) {
	if value != nil {
		conn.Send("incrby", key, value)
	}
	if expire > 0 {
		conn.Send("expire", key, expire)
	}
	conn.Flush()
	ret, err := redis.Int64(conn.Receive())
	if err != nil {
		logger.Warnf("redis incr key:%s,value:%v,expire:%d,err:%v", key, value, expire, err)
	}
	return ret, err
}

// GetRedis 泛型获得,没有这个key的时候也返回error
func GetRedis(conn RedisConn, key string, valuePtr interface{}) error {
	ret, err := conn.Do("get", key)
	//ret, err := redis.String(conn.Do("get", key))
	if err == nil && ret != nil {
		var formaterr error
		switch out := valuePtr.(type) {
		case *string:
			*out, formaterr = redis.String(ret, err)
			break
		case *int:
			*out, formaterr = redis.Int(ret, err)
			break
		case *int32:
			var ret32 int
			ret32, formaterr = redis.Int(ret, err)
			*out = int32(ret32)
			break
		case *int64:
			*out, formaterr = redis.Int64(ret, err)
			break
		case *uint64:
			*out, formaterr = redis.Uint64(ret, err)
			break
		case *float64:
			*out, formaterr = redis.Float64(ret, err)
			break
		case *float32:
			var ret64 float64
			ret64, formaterr = redis.Float64(ret, err)
			*out = float32(ret64)
			break
		case *[]byte:
			*out, formaterr = redis.Bytes(ret, err)
			break
		case *bool:
			*out, formaterr = redis.Bool(ret, err)
		case proto.Message:
			var bytes []byte
			bytes, formaterr = redis.Bytes(ret, err)
			proto.Unmarshal(bytes, out)
			break
		default:
			logger.Fatalln("can't support type")
			break
		}
		if formaterr != nil {
			return fmt.Errorf("type error when db.get")
		}
		return nil
	}
	return fmt.Errorf("value not found")
}

// IncrRedis incr value
func IncrRedis(conn RedisConn, key string, step interface{}) (int64, error) {
	switch step.(type) {
	case int:
		return redis.Int64(conn.Do("incrby", key, step))
	case int64:
		return redis.Int64(conn.Do("incrby", key, step))
	case int32:
		return redis.Int64(conn.Do("incrby", key, step))
	case uint:
		return redis.Int64(conn.Do("incrby", key, step))
	case uint32:
		return redis.Int64(conn.Do("incrby", key, step))
	case uint64:
		return redis.Int64(conn.Do("incrby", key, step))
	default:
		logger.Fatalln("can't support type")
		return 0, fmt.Errorf("can't support type")
	}
}

// IncrHashRedis incr hash value
func IncrHashRedis(conn RedisConn, hkey string, key string, step interface{}) (int64, error) {
	switch step.(type) {
	case int:
		return redis.Int64(conn.Do("hincrby", hkey, key, step))
	case int64:
		return redis.Int64(conn.Do("hincrby", hkey, key, step))
	case int32:
		return redis.Int64(conn.Do("hincrby", hkey, key, step))
	case uint:
		return redis.Int64(conn.Do("hincrby", hkey, key, step))
	case uint32:
		return redis.Int64(conn.Do("hincrby", hkey, key, step))
	case uint64:
		return redis.Int64(conn.Do("hincrby", hkey, key, step))
	default:
		logger.Fatalln("can't support type")
		return 0, fmt.Errorf("can't support type")
	}
}

// SetRedis set a value by interface
func SetRedis(conn RedisConn, key string, valuePtr interface{}) {
	switch out := valuePtr.(type) {
	case *int:
		conn.Do("set", key, *out)
	case *int32:
		conn.Do("set", key, *out)
	case *int64:
		conn.Do("set", key, *out)
	case *float32:
		conn.Do("set", key, *out)
	case *string:
		conn.Do("set", key, *out)
	case *float64:
		conn.Do("set", key, *out)
	case *[]byte:
		conn.Do("set", key, *out)
	case *bool:
		conn.Do("set", key, *out)
		break
	case int:
		conn.Do("set", key, out)
	case int32:
		conn.Do("set", key, out)
	case uint64:
		conn.Do("set", key, out)
	case int64:
		conn.Do("set", key, out)
	case float32:
		conn.Do("set", key, out)
	case string:
		conn.Do("set", key, out)
	case float64:
		conn.Do("set", key, out)
	case []byte:
		conn.Do("set", key, out)
	case bool:
		conn.Do("set", key, out)
		break
	case proto.Message:
		buf, err := proto.Marshal(out)
		if err == nil {
			conn.Do("set", key, buf)
		}
		break
	default:
		logger.Fatalln("can't support type")
		break
	}

}

// SetHashRedis set hash value
func SetHashRedis(conn RedisConn, hashKey string, key string, valuePtr interface{}) (reply interface{}, err error) {
	switch out := valuePtr.(type) {
	case *int:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case *int32:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case *int64:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case *uint64:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case *float32:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case *string:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case *float64:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case *[]byte:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case *bool:
		reply, err = conn.Do("hset", hashKey, key, *out)
	case int:
		reply, err = conn.Do("hset", hashKey, key, out)
	case int32:
		reply, err = conn.Do("hset", hashKey, key, out)
	case int64:
		reply, err = conn.Do("hset", hashKey, key, out)
	case uint64:
		reply, err = conn.Do("hset", hashKey, key, out)
	case float32:
		reply, err = conn.Do("hset", hashKey, key, out)
	case string:
		reply, err = conn.Do("hset", hashKey, key, out)
	case float64:
		reply, err = conn.Do("hset", hashKey, key, out)
	case []byte:
		reply, err = conn.Do("hset", hashKey, key, out)
	case bool:
		reply, err = conn.Do("hset", hashKey, key, out)
	case proto.Message:
		var buf []byte
		buf, err = proto.Marshal(out)
		if err == nil {
			reply, err = conn.Do("hset", hashKey, key, buf)
		}
	}
	return
}

// KeyExistsRedis 是否存在这个键值
func KeyExistsRedis(conn RedisConn, key string) bool {
	exist, err := redis.Bool(conn.Do("exists", key))
	if exist && err == nil {
		return true
	}
	return false
}

// GenKeyIDRedis 获得一个自增长id
func GenKeyIDRedis(conn RedisConn, key string, start uint64) uint64 {
	exist, err := redis.Bool(conn.Do("exists", key))
	if exist && err == nil {
		var retv uint64
		retv, err = redis.Uint64(conn.Do("incrby", key, 1))
		if err == nil {
			return retv
		}
		logger.Fatalf("genid (%s)Error(%v)", key, err)
		return 0
	}
	_, err = conn.Do("set", key, start)
	if err == nil {
		return start
	}
	logger.Fatalf("genid (%s)Error(%s)", key, err.Error())
	return 0
}

// DelRedis 删除某个键值
func DelRedis(conn RedisConn, key string) {
	conn.Do("del", key)
}

// HExistsRedis 是否存在某个hash值
func HExistsRedis(conn RedisConn, hashKey, key string) bool {
	exist, err := redis.Bool(conn.Do("hexists", hashKey, key))
	if exist && err == nil {
		return true
	}

	return false
}

// HashDelRedis 删除某个hash键值
func HashDelRedis(conn RedisConn, hashKey, key string) (reply interface{}, err error) {
	reply, err = conn.Do("hdel", hashKey, key)
	return
}

// HLenRedis hash长度
func HLenRedis(conn RedisConn, hashKey string) uint {
	res, err := conn.Do("hlen", hashKey)
	if err == nil {
		len, err := redis.Int(res, err)
		if len != 0 && err == nil {
			return uint(len)
		}
	}
	return 0
}
