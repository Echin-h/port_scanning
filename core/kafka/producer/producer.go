package producer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"port_scanning/core/message"
)

// 单例模式
var (
	instance *Producer
	once     sync.Once
	mu       sync.RWMutex
)

// IProducer 接口定义了生产者的行为
type IProducer interface {
	Produce(ctx context.Context, msg message.Message) error
	ProduceAsync(ctx context.Context, msg message.Message, callback func(error))
	ProduceBatch(ctx context.Context, msg []message.Message) error
	ProduceToTopic(ctx context.Context, topic string, key string, value []byte) error
	Close() error
}

// Producer 生产者
type Producer struct {
	writer *kafka.Writer // 单个writer
	addr   []string      // Kafka地址
	closed bool
	mu     sync.RWMutex
	config ProducerConfig // 新增配置结构
}

// GetProducer 获取单例，支持自定义配置
func GetProducer() (*Producer, error) {
	mu.RLock()
	if instance == nil {
		mu.RUnlock()
		return nil, errors.New("kafka producer not initialized, please call NewProducer first")
	}
	mu.RUnlock()

	return instance, nil
}

func NewProducer(addr []string, opts ...ProducerOption) (*Producer, error) {
	var _err error
	once.Do(func() {
		instance, _err = newKafkaProducer(addr, opts...)
	})
	return instance, _err
}

// newKafkaProducer 创建KafkaProducer实例
func newKafkaProducer(addr []string, opts ...ProducerOption) (*Producer, error) {
	if len(addr) == 0 {
		return nil, errors.New("kafka broker addresses cannot be empty")
	}

	config := defaultProducerConfig()

	// 应用自定义配置选项
	for _, opt := range opts {
		opt(&config)
	}

	// 创建 kafka.Writer，不设置 Topic（在发送时动态设置）
	writer := &kafka.Writer{
		Addr:         kafka.TCP(addr...),
		RequiredAcks: kafka.RequiredAcks(config.RequiredAcks),
		Async:        config.Async,
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		Balancer:     config.Balancer,
	}

	kp := &Producer{
		writer: writer,
		addr:   addr,
		closed: false,
		config: config,
	}

	return kp, nil
}

// Produce 同步发布消息
func (kp *Producer) Produce(ctx context.Context, msg message.Message) error {
	if err := msg.Validate(); err != nil {
		log.Println("Message validation failed:", err)
		return err
	}
	return kp.ProduceToTopic(ctx, msg.GetTopic(), msg.GetKey(), msg.GetValue())
}

// ProduceAsync 异步发布消息
func (kp *Producer) ProduceAsync(ctx context.Context, msg message.Message, callback func(error)) {
	go func() {
		err := kp.Produce(ctx, msg)
		if callback != nil {
			callback(err)
		}
	}()
}

// ProduceBatch 批量发布消息
func (kp *Producer) ProduceBatch(ctx context.Context, msg []message.Message) error {
	if len(msg) == 0 {
		log.Println("No messages to produce in batch")
		return nil
	}

	kp.mu.RLock()
	if kp.closed {
		kp.mu.RUnlock()
		return errors.New("producer is closed")
	}
	kp.mu.RUnlock()

	// 验证所有消息
	for _, m := range msg {
		if err := m.Validate(); err != nil {
			log.Println("Message validation failed:", err)
			return err
		}
	}

	// 转换为 kafka.Message 格式
	var kafkaMessages []kafka.Message
	for _, m := range msg {
		kafkaMsg := kafka.Message{
			Topic: m.GetTopic(), // 在消息中设置 topic
			Key:   []byte(m.GetKey()),
			Value: m.GetValue(),
			Headers: []kafka.Header{
				{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
			},
			Time: time.Now(),
		}
		kafkaMessages = append(kafkaMessages, kafkaMsg)
	}

	// 批量发送消息
	if err := kp.writer.WriteMessages(ctx, kafkaMessages...); err != nil {
		return fmt.Errorf("failed to write batch messages: %w", err)
	}

	log.Printf("Successfully produced %d messages in batch", len(msg))
	return nil
}

// ProduceToTopic 直接向指定topic发布消息
func (kp *Producer) ProduceToTopic(ctx context.Context, topic string, key string, value []byte) error {
	kp.mu.RLock()
	if kp.closed {
		kp.mu.RUnlock()
		return errors.New("producer is closed")
	}
	kp.mu.RUnlock()

	kafkaMsg := kafka.Message{
		Topic: topic, // 在消息中设置 topic
		Key:   []byte(key),
		Value: value,
		Headers: []kafka.Header{
			{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
		},
		Time: time.Now(),
	}

	err := kp.writer.WriteMessages(ctx, kafkaMsg)
	if err != nil {
		log.Println("Failed to write message to topic:", topic, "Error:", err)
		return err
	}

	log.Printf("消息%s 成功发布到主题: %s", key, topic)
	return nil
}

// Close 关闭发布器
func (kp *Producer) Close() error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.closed {
		return nil
	}

	kp.closed = true

	// 关闭 writer
	if kp.writer != nil {
		if err := kp.writer.Close(); err != nil {
			log.Printf("Error closing writer: %v", err)
			return err
		}
		kp.writer = nil
	}

	// 清空地址列表
	kp.addr = nil

	// 释放实例
	mu.Lock()
	instance = nil
	mu.Unlock()

	log.Println("KafkaProducer closed successfully")
	return nil
}
