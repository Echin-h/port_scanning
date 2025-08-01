package producer

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// ProducerOption 配置选项函数
type ProducerOption func(*ProducerConfig)

// ProducerConfig 生产者配置
type ProducerConfig struct {
	RequiredAcks      int
	Async             bool
	BatchSize         int
	BatchTimeout      time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	Balancer          kafka.Balancer
	AutoTopicCreation bool
}

// 默认配置
func defaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		RequiredAcks:      -1,
		Async:             false,
		BatchSize:         16384,
		BatchTimeout:      1 * time.Millisecond,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		Balancer:          &kafka.Hash{},
		AutoTopicCreation: false,
	}
}

// WithRequiredAcks 设置确认模式
func WithRequiredAcks(acks int) ProducerOption {
	return func(config *ProducerConfig) {
		config.RequiredAcks = acks
	}
}

// WithAsync 设置是否异步发送
func WithAsync(async bool) ProducerOption {
	return func(config *ProducerConfig) {
		config.Async = async
	}
}

// WithBatchSize 设置批次大小
func WithBatchSize(size int) ProducerOption {
	return func(config *ProducerConfig) {
		config.BatchSize = size
	}
}

// WithBatchTimeout 设置批次超时时间
func WithBatchTimeout(timeout time.Duration) ProducerOption {
	return func(config *ProducerConfig) {
		config.BatchTimeout = timeout
	}
}

// WithProducerWriteTimeout 设置写入超时时间
func WithProducerWriteTimeout(timeout time.Duration) ProducerOption {
	return func(config *ProducerConfig) {
		config.WriteTimeout = timeout
	}
}

// WithProducerReadTimeout 设置读取超时时间
func WithProducerReadTimeout(timeout time.Duration) ProducerOption {
	return func(config *ProducerConfig) {
		config.ReadTimeout = timeout
	}
}

// WithBalancer 设置负载均衡器
func WithBalancer(balancer kafka.Balancer) ProducerOption {
	return func(config *ProducerConfig) {
		config.Balancer = balancer
	}
}

func WithAutoTopicCreation(enabled bool) ProducerOption {
	return func(config *ProducerConfig) {
		config.AutoTopicCreation = enabled
	}
}
