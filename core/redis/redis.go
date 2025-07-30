package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type IRedisClient interface {
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, keys ...string) error
	Exists(ctx context.Context, keys ...string) (int64, error)
	HSet(ctx context.Context, key string, values ...interface{}) error
	HGet(ctx context.Context, key, field string) (string, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HDel(ctx context.Context, key string, fields ...string) error
	LPush(ctx context.Context, key string, values ...interface{}) error
	RPop(ctx context.Context, key string) (string, error)
	LLen(ctx context.Context, key string) (int64, error)
	Expire(ctx context.Context, key string, expiration time.Duration) error
	TTL(ctx context.Context, key string) (time.Duration, error)
	Ping(ctx context.Context) error
	Close() error
}

var rds IRedisClient

type RedisClient struct {
	client *redis.Client
	config *RedisConfig
}

func NewRedisClient(config *RedisConfig) {
	if config == nil {
		config = DefaultRedisConfig()
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Host, config.Port),
		Password: config.Password,
		DB:       config.DB,
	})

	rds = &RedisClient{
		client: rdb,
		config: config,
	}
}

func GetRedisClient() IRedisClient {
	return rds
}

// Set 设置键值
func (r *RedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	err := r.client.Set(ctx, key, value, expiration).Err()
	return err
}

// Get 获取值
func (r *RedisClient) Get(ctx context.Context, key string) (string, error) {
	result, err := r.client.Get(ctx, key).Result()
	return result, err
}

// Del 删除键
func (r *RedisClient) Del(ctx context.Context, keys ...string) error {
	err := r.client.Del(ctx, keys...).Err()
	return err
}

// Exists 检查键是否存在
func (r *RedisClient) Exists(ctx context.Context, keys ...string) (int64, error) {
	result, err := r.client.Exists(ctx, keys...).Result()
	return result, err
}

// HSet 设置哈希字段
func (r *RedisClient) HSet(ctx context.Context, key string, values ...interface{}) error {
	err := r.client.HSet(ctx, key, values...).Err()
	return err
}

// HGet 获取哈希字段值
func (r *RedisClient) HGet(ctx context.Context, key, field string) (string, error) {
	result, err := r.client.HGet(ctx, key, field).Result()
	return result, err
}

// HGetAll 获取所有哈希字段
func (r *RedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	result, err := r.client.HGetAll(ctx, key).Result()
	return result, err
}

// HDel 删除哈希字段
func (r *RedisClient) HDel(ctx context.Context, key string, fields ...string) error {
	err := r.client.HDel(ctx, key, fields...).Err()
	return err
}

// LPush 从左侧推入列表
func (r *RedisClient) LPush(ctx context.Context, key string, values ...interface{}) error {
	err := r.client.LPush(ctx, key, values...).Err()
	return err
}

// RPop 从右侧弹出列表元素
func (r *RedisClient) RPop(ctx context.Context, key string) (string, error) {
	result, err := r.client.RPop(ctx, key).Result()
	return result, err
}

// LLen 获取列表长度
func (r *RedisClient) LLen(ctx context.Context, key string) (int64, error) {
	result, err := r.client.LLen(ctx, key).Result()
	return result, err
}

// Expire 设置过期时间
func (r *RedisClient) Expire(ctx context.Context, key string, expiration time.Duration) error {
	err := r.client.Expire(ctx, key, expiration).Err()
	return err
}

// TTL 获取剩余生存时间
func (r *RedisClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	result, err := r.client.TTL(ctx, key).Result()
	return result, err
}

// Ping 测试连接
func (r *RedisClient) Ping(ctx context.Context) error {
	err := r.client.Ping(ctx).Err()
	return err
}

// Close 关闭连接
func (r *RedisClient) Close() error {
	return r.client.Close()
}
