package consumer

import (
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// ConsumerOption 消费者配置选项函数
type ConsumerOption func(*kafka.ReaderConfig)

// ConsumerConfig Kafka消费者配置结构
type ConsumerConfig struct {
	Brokers          []string
	GroupID          string
	StartOffset      int64
	MinBytes         int
	MaxBytes         int
	MaxWait          time.Duration
	ReadBatchTimeout time.Duration
	CommitInterval   time.Duration
}

type AsyncConfig struct {
	WorkerCount int
	QueueSize   int
}

// 默认消费者配置
func defaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		GroupID:          "scan-consumer-group",
		StartOffset:      kafka.LastOffset,
		MinBytes:         1,
		MaxBytes:         10e6, // 10MB
		MaxWait:          1 * time.Second,
		ReadBatchTimeout: 10 * time.Second,
		CommitInterval:   1 * time.Second,
	}
}

// NewConsumerConfig 获取消费者配置（支持选项函数）
func NewConsumerConfig(broker []string, opts ...ConsumerOption) (*ConsumerConfig, error) {
	if broker == nil {
		return nil, errors.New("broker cannot be empty")
	}

	config := defaultConsumerConfig()

	readerConfig := kafka.ReaderConfig{
		Brokers:        broker,
		GroupID:        config.GroupID,
		StartOffset:    config.StartOffset,
		MinBytes:       config.MinBytes,
		MaxBytes:       config.MaxBytes,
		MaxWait:        config.MaxWait,
		CommitInterval: config.CommitInterval,
	}

	for _, opt := range opts {
		opt(&readerConfig)
	}

	return &ConsumerConfig{
		Brokers:        readerConfig.Brokers,
		GroupID:        readerConfig.GroupID,
		StartOffset:    readerConfig.StartOffset,
		MinBytes:       readerConfig.MinBytes,
		MaxBytes:       readerConfig.MaxBytes,
		MaxWait:        readerConfig.MaxWait,
		CommitInterval: readerConfig.CommitInterval,
	}, nil
}

// GetReaderConfig 获取 kafka.ReaderConfig
func (c *ConsumerConfig) GetReaderConfig(topic string) kafka.ReaderConfig {
	config := kafka.ReaderConfig{
		Brokers:        c.Brokers,
		Topic:          topic,
		GroupID:        c.GroupID,
		StartOffset:    c.StartOffset,
		MinBytes:       c.MinBytes,
		MaxBytes:       c.MaxBytes,
		MaxWait:        c.MaxWait,
		CommitInterval: c.CommitInterval,
	}

	return config
}

// CreateReaderForTopic 为指定主题创建 Reader
func (c *ConsumerConfig) CreateReaderForTopic(topic string) *kafka.Reader {
	//config := kafka.ReaderConfig{
	//	Brokers:  []string{"localhost:9092"},
	//	GroupID:  "scan-consumer-group",
	//	Topic:    "scan-tasks",
	//	MaxBytes: 10e6, // 10MB
	//}
	//return kafka.NewReader(config)
	config := c.GetReaderConfig(topic)
	return kafka.NewReader(config)
}

// GetBrokers 获取Broker列表
func (c *ConsumerConfig) GetBrokers() []string {
	return c.Brokers
}

// GetGroupID 获取消费者组ID
func (c *ConsumerConfig) GetGroupID() string {
	return c.GroupID
}

// WithGroupID 设置消费者组ID
func WithGroupID(groupID string) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.GroupID = groupID
	}
}

// WithStartOffset 设置起始偏移量
func WithStartOffset(offset int64) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.StartOffset = offset
	}
}

// WithEarliestOffset 从最早的消息开始消费
func WithEarliestOffset() ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.StartOffset = kafka.FirstOffset
	}
}

// WithLatestOffset 从最新的消息开始消费
func WithLatestOffset() ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.StartOffset = kafka.LastOffset
	}
}

// WithMinBytes 设置最小读取字节数
func WithMinBytes(minBytes int) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.MinBytes = minBytes
	}
}

// WithMaxBytes 设置最大读取字节数
func WithMaxBytes(maxBytes int) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.MaxBytes = maxBytes
	}
}

// WithMaxWait 设置最大等待时间
func WithMaxWait(maxWait time.Duration) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.MaxWait = maxWait
	}
}

// WithCommitInterval 设置提交间隔
func WithCommitInterval(interval time.Duration) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.CommitInterval = interval
	}
}

// WithLogger 设置日志记录器
func WithLogger(logger kafka.Logger) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.Logger = logger
	}
}

func WithDefaultLogger() ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.Logger = kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Println(msg, args)
		})
	}
}

func WithDefaultErrorLogger() ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.ErrorLogger = kafka.LoggerFunc(func(msg string, args ...interface{}) {
			log.Println("ERROR:", msg, args)
		})
	}
}

// WithErrorLogger 设置错误日志记录器
func WithErrorLogger(errorLogger kafka.Logger) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.ErrorLogger = errorLogger
	}
}

// WithDialer 设置自定义拨号器
func WithDialer(dialer *kafka.Dialer) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.Dialer = dialer
	}
}

func WithReadBatchTimeout(timeout time.Duration) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.ReadBatchTimeout = timeout
	}
}

func WithRoundRobinGroupBalancer() ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.GroupBalancers = []kafka.GroupBalancer{
			kafka.RoundRobinGroupBalancer{},
		}
	}
}

func WithHeartbeatInterval(interval time.Duration) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.HeartbeatInterval = interval
	}
}

func WithSessionTimeout(timeout time.Duration) ConsumerOption {
	return func(config *kafka.ReaderConfig) {
		config.SessionTimeout = timeout
	}
}
