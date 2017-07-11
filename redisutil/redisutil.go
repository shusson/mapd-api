package redisutil

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

// NewPool construct a new redis pool
func NewPool(redisURL string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 100 * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", redisURL)
			if err != nil {
				return nil, err
			}
			return conn, err
		},
	}
}

// Get redis get
func Get(pool *redis.Pool, key string) ([]byte, error) {
	conn := pool.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("GET", key))
}

// Set redis set
func Set(pool *redis.Pool, key string, value []byte) error {
	conn := pool.Get()
	defer conn.Close()

	_, err := conn.Do("SET", key, value)
	return err
}