package config

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// KafkaConfig Kafka配置结构
type KafkaConfig struct {
	Brokers []string
	GroupID string
}

var (
	instance *KafkaConfig
	once     sync.Once
)

// GetKafkaConfig 获取Kafka配置单例
func GetKafkaConfig() *KafkaConfig {
	once.Do(func() {
		instance = &KafkaConfig{
			Brokers: getBrokers(),
			GroupID: getEnvOrDefault("KAFKA_GROUP_ID", "scan-consumer-group"),
		}
	})
	return instance
}

// getBrokers 获取Broker列表
func getBrokers() []string {
	brokersStr := getEnvOrDefault("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersStr, ",")

	// 清理空格
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
	}

	return brokers
}

// GetProducerConfig 获取生产者配置
func (c *KafkaConfig) GetProducerConfig(topic string) kafka.WriterConfig {
	return kafka.WriterConfig{
		Brokers:      c.Brokers,
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},  // 负载均衡策略
		RequiredAcks: -1,                   // 等同于 "acks": "all"
		Async:        false,                // 同步发送
		BatchSize:    16384,                // 批次大小
		BatchTimeout: 1 * time.Millisecond, // 等同于 "linger.ms": 1
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
}

// GetConsumerConfig 获取消费者配置
func (c *KafkaConfig) GetConsumerConfig(topic string) kafka.ReaderConfig {
	return kafka.ReaderConfig{
		Brokers:     c.Brokers,
		Topic:       topic,
		GroupID:     c.GroupID,
		StartOffset: kafka.FirstOffset, // 等同于 "auto.offset.reset": "earliest"
		MinBytes:    1,                 // 最小读取字节数
		MaxBytes:    10e6,              // 最大读取字节数 (10MB)
		MaxWait:     1 * time.Second,   // 最大等待时间
	}
}

// GetGroupConsumerConfig 获取消费者组配置
func (c *KafkaConfig) GetGroupConsumerConfig(topics []string) kafka.ConsumerGroupConfig {
	return kafka.ConsumerGroupConfig{
		ID:      c.GroupID,
		Brokers: c.Brokers,
		Topics:  topics,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		StartOffset: kafka.FirstOffset,
	}
}

// GetWriterForTopic 为指定主题创建 Writer
func (c *KafkaConfig) GetWriterForTopic(topic string) *kafka.Writer {
	config := c.GetProducerConfig(topic)
	return kafka.NewWriter(config)
}

// GetReaderForTopic 为指定主题创建 Reader
func (c *KafkaConfig) GetReaderForTopic(topic string) *kafka.Reader {
	config := c.GetConsumerConfig(topic)
	return kafka.NewReader(config)
}

// GetDialer 获取连接器（支持认证）
func (c *KafkaConfig) GetDialer() *kafka.Dialer {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// 如果配置了 SASL 认证
	if username := getEnvOrDefault("KAFKA_SASL_USERNAME", ""); username != "" {
		password := getEnvOrDefault("KAFKA_SASL_PASSWORD", "")
		dialer.SASLMechanism = plain.Mechanism{
			Username: username,
			Password: password,
		}
	}

	return dialer
}

// getBrokerString 获取Broker字符串（保持兼容性）
func (c *KafkaConfig) getBrokerString() string {
	return strings.Join(c.Brokers, ",")
}

// GetBrokers 获取Broker列表
func (c *KafkaConfig) GetBrokers() []string {
	return c.Brokers
}

// GetGroupID 获取消费者组ID
func (c *KafkaConfig) GetGroupID() string {
	return c.GroupID
}

// 辅助函数
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

//// 主题配置
//type TopicConfig struct {
//	ScanTasks    string
//	ScanResults  string
//	ScanProgress string
//}
//
//// GetTopicConfig 获取主题配置
//func GetTopicConfig() TopicConfig {
//	return TopicConfig{
//		ScanTasks:    getEnvOrDefault("KAFKA_TOPIC_SCAN_TASKS", "scan-tasks"),
//		ScanResults:  getEnvOrDefault("KAFKA_TOPIC_SCAN_RESULTS", "scan-results"),
//		ScanProgress: getEnvOrDefault("KAFKA_TOPIC_SCAN_PROGRESS", "scan-progress"),
//	}
//}
