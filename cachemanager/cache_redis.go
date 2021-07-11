package cachemanager

/*

Provides cache access and manipulation methods for redis server
Implements cacher interface.

Official docs -
1. redis client - https://github.com/go-redis/redis
2. redis server - https://redis.io/



*/

import (
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"
	"context"
	"github.com/crearosoft/corelib/loggermanager"

	"github.com/go-redis/redis/v8"
)

const (
	noExp time.Duration = 0
	keySplitter = ":"
)
var ctx = context.Background()

// RedisCache represents a Redis client with provided configuration. Do not change configuration at runtime.
type RedisCache struct {
	cli       *redis.Client  // represents redis client
	opt       *redis.Options //
	keyStr    string         // "<Prefix>:"
	addPrefix bool           //
	connected bool           // will be enabled if redis client connects to server

	Addr       string        // redis server address, default "127.0.0.1:6379"
	DB         int           // redis DB on provided server, default 0
	Password   string        //
	Expiration time.Duration // this duration will be used for Set() method
	Prefix     string        // this will be used for storing keys for provided project
}

type configRedis struct {
	addr       string        // redis server address, default "127.0.0.1:6379"
	db         int           // redis DB on provided server, default 0
	password   string        //
	expiration time.Duration // this duration will be used for Set() method
	prefix     string        // this will be used for storing keys for provided project
}

type redisOption func(*configRedis)

func RedisWithAddr(addr string) redisOption {
	return func(cfg *configRedis) {
		cfg.addr = addr
	}
}
func RedisWithDB(db int) redisOption {
	return func(cfg *configRedis) {
		cfg.db = db
	}
}
func RedisWithPrefix(pfx string) redisOption {
	return func(cfg *configRedis) {
		cfg.prefix = pfx
	}
}
func RedisWithPassword(p string) redisOption {
	return func(cfg *configRedis) {
		cfg.password = p
	}
}
func RedisWithExpiration(exp time.Duration) redisOption {
	return func(cfg *configRedis) {
		cfg.expiration = exp
	}
}

// Setup initializes redis cache for application. Must be called only once.
func (rc *RedisCache) Setup(addr, password, prefix string, db int, exp time.Duration) {

	if rc == nil {
		rc = new(RedisCache)
	}

	rc.Addr = addr
	rc.Password = password
	rc.DB = db
	rc.Expiration = exp
	rc.Prefix = prefix
	opt := redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	}

	rc.opt = &opt
	rc.cli = redis.NewClient(&opt)

	if _, err := rc.cli.Ping(ctx).Result(); err != nil {
		// exit if connection to redis server fails
		loggermanager.LogError("connection to redis server failed: ", err)
		log.Fatal("connection to redis server failed: ", err)
	}

	rc.connected = true

	if prefix != "" {
		rc.keyStr = contcat(rc.Prefix, keySplitter)
		rc.addPrefix = true
	}

}

// SetupRedisCache initializes redis cache for application and returns it. Must be called only once.
func SetupRedisCache(opts ...redisOption) (*RedisCache, error) {

	rc := new(RedisCache)

	cfg := new(configRedis)

	for i := range opts {
		opts[i](cfg)
	}

	rc.Addr = cfg.addr
	rc.Password = cfg.password
	rc.DB = cfg.db
	rc.Expiration = cfg.expiration
	rc.Prefix = cfg.prefix

	rc.opt = &redis.Options{
		Addr:     cfg.addr,
		Password: cfg.password,
		DB:       cfg.db,
	}

	rc.cli = redis.NewClient(rc.opt)

	if _, err := rc.cli.Ping(ctx).Result(); err != nil {

		return nil, errors.New("connection to redis server failed: " + err.Error())
	}

	rc.connected = true

	if cfg.prefix != "" {
		rc.keyStr = contcat(rc.Prefix, keySplitter)
		rc.addPrefix = true
	}

	return rc, nil
}

// Set marshalls provided value and stores against provided key. Errors will be logged to initialized logger.
func (rc *RedisCache) Set(key string, val interface{}) {
	ba, err := marshalWithTypeCheck(val)
	if err != nil {
		loggermanager.LogError("error setting key ", key, " error: ", err)
		return
	}

	rc.cli.Set(ctx,rc.key(key), ba, rc.Expiration)
}

// SetWithExpiration marshalls provided value and stores against provided key for given duration. Errors will be logged to initialized logger.
func (rc *RedisCache) SetWithExpiration(key string, val interface{}, exp time.Duration) {
	ba, err := marshalWithTypeCheck(val)
	if err != nil {
		loggermanager.LogError("error setting key ", key, " error: ", err)
		return
	}

	rc.cli.Set(ctx,rc.key(key), ba, exp)
}

// SetNoExpiration marshalls provided value and stores against provided key.
// Errors will be logged to initialized logger.
func (rc *RedisCache) SetNoExpiration(key string, val interface{}) {
	ba, err := marshalWithTypeCheck(val)
	if err != nil {
		loggermanager.LogError("error setting key ", key, " error: ", err)
		return
	}

	rc.cli.Set(ctx,rc.key(key), ba, noExp)
}

// Get returns data against provided key. Returns false if not present.
func (rc *RedisCache) Get(key string) (interface{}, bool) {

	// Get returns error if key is not present.
	val, err := rc.cli.Get(ctx,rc.key(key)).Result()
	if err != nil {
		loggermanager.LogError("error getting key", key, "from redis cache with error:", err)
		return nil, false
	}

	return val, true
}

// Delete -
func (rc *RedisCache) Delete(key string) {
	rc.cli.Del(ctx,rc.key(key)).Result()
}

// GetItemsCount -
func (rc *RedisCache) GetItemsCount() int {
	// pattern := rc.Prefix + "*"
	// keys, err := rc.cli.Keys(pattern).Result()
	// if err != nil {
	// 	loggermanager.LogError("error getting item count for ", pattern, " error: ", err)
	// 	return 0
	// }
	return len(rc.keys())
}

func (rc *RedisCache) flushDB() (string, error) {
	return rc.cli.FlushDB(ctx).Result()
}

// Purge deletes for current redis db
func (rc *RedisCache) Purge() {
	_, err := rc.flushDB()
	if err != nil {
		loggermanager.LogError("error purging redis cache for db ", rc.Addr, "/", rc.DB, " error: ", err)
	}
}

func marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// marshalWithTypeCheck checks type before marshsal. Save allocations and time significantly if the existing data is string or []byte
func marshalWithTypeCheck(v interface{}) ([]byte, error) {
	switch d := v.(type) {
	default:
		return json.Marshal(v)
	case string:
		return []byte(d), nil
	case []byte:
		return d, nil
	}
}

func contcat(s ...string) string {
	sb := strings.Builder{}
	for i := range s {
		sb.WriteString(s[i])
	}

	return sb.String()
}

func (rc *RedisCache) key(key string) string {
	// prepare in format "<Prefix>:<key>"
	if rc.addPrefix {
		return contcat(rc.keyStr, key)
	}
	return key
}

func (rc *RedisCache) actualKey(key string) string {
	if rc.addPrefix {
		return strings.TrimPrefix(key, rc.keyStr)
	}
	return key
}

func (rc *RedisCache) Type() int {
	return TypeRedisCache
}

// GetAll returns all keys with values present in redis server. Excludes the keys which does not have specified prefix. If prefix is empty, then returns all keys.
//
// **This is not intended for production use. May hamper performance**
func (rc *RedisCache) GetAll() map[string]interface{} {
	keys := rc.keys()

	result := make(map[string]interface{}, len(keys))

	for i := range keys {
		ba, err := rc.cli.Get(ctx,keys[i]).Bytes()
		if err != nil {
			loggermanager.LogError("error getting key", keys[i], "from redis cache with error:", err)
			continue
		}

		var val interface{}
		_ = json.Unmarshal(ba, &val)

		result[rc.actualKey(keys[i])] = val
	}

	return result
}

// GetItemsCount -
func (rc *RedisCache) keys() []string {
	pattern := rc.Prefix + "*"
	keys, err := rc.cli.Keys(ctx,pattern).Result()
	if err != nil {
		loggermanager.LogError("error getting item count for ", pattern, " error: ", err)
	}
	return keys
}
