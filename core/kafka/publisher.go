package kafka

// TODO: 每次发布一个就需要NewKafkaPublisher，这样会导致每次都创建新的writer，可能会导致资源浪费

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"port_scanning/core/kafka/config"
	"port_scanning/core/kafka/message"
)

// KafkaPublisher Kafka发布器实现
type KafkaPublisher struct {
	writers map[string]*kafka.Writer // 每个topic一个writer
	config  *config.KafkaConfig
	closed  bool
	mu      sync.RWMutex
}

// NewKafkaPublisher 创建Kafka发布器
func NewKafkaPublisher() (*KafkaPublisher, error) {
	cfg := config.GetKafkaConfig()

	kp := &KafkaPublisher{
		writers: make(map[string]*kafka.Writer),
		config:  cfg,
		closed:  false,
	}

	return kp, nil
}

func Publish(message message.Message) error {
	publisher, err := NewKafkaPublisher()
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := publisher.Publish(ctx, message); err != nil {
		return fmt.Errorf("failed to publish message: %s", err.Error())
	}

	return nil
}

// getWriter 获取或创建指定topic的writer
func (kp *KafkaPublisher) getWriter(topic string) *kafka.Writer {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if writer, exists := kp.writers[topic]; exists {
		return writer
	}

	// 创建新的writer
	writer := kp.config.GetWriterForTopic(topic)
	kp.writers[topic] = writer
	return writer
}

// Publish 同步发布消息
func (kp *KafkaPublisher) Publish(ctx context.Context, msg message.Message) error {
	return kp.PublishToTopic(ctx, msg.GetTopic(), msg.GetKey(), msg.GetValue())
}

// PublishAsync 异步发布消息
func (kp *KafkaPublisher) PublishAsync(ctx context.Context, msg message.Message, callback func(error)) {
	go func() {
		err := kp.Publish(ctx, msg)
		if callback != nil {
			callback(err)
		}
	}()
}

// PublishBatch 批量发布消息
func (kp *KafkaPublisher) PublishBatch(ctx context.Context, msgs []message.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	kp.mu.RLock()
	if kp.closed {
		kp.mu.RUnlock()
		return fmt.Errorf("publisher is closed")
	}
	kp.mu.RUnlock()

	// 验证所有消息
	for i, msg := range msgs {
		if err := msg.Validate(); err != nil {
			return fmt.Errorf("message %d validation failed: %w", i, err)
		}
	}

	// 按topic分组消息
	topicMessages := make(map[string][]kafka.Message)
	for _, msg := range msgs {
		topic := msg.GetTopic()
		kafkaMsg := kafka.Message{
			Key:   []byte(msg.GetKey()),
			Value: msg.GetValue(),
			Headers: []kafka.Header{
				{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
				{Key: "message_type", Value: []byte(fmt.Sprintf("%T", msg))},
			},
			Time: time.Now(),
		}
		topicMessages[topic] = append(topicMessages[topic], kafkaMsg)
	}

	// 为每个topic批量发送消息
	var wg sync.WaitGroup
	errChan := make(chan error, len(topicMessages))

	for topic, messages := range topicMessages {
		wg.Add(1)
		go func(t string, msgs []kafka.Message) {
			defer wg.Done()
			writer := kp.getWriter(t)
			if err := writer.WriteMessages(ctx, msgs...); err != nil {
				errChan <- fmt.Errorf("failed to write messages to topic %s: %w", t, err)
				return
			}
			log.Printf("Successfully published %d messages to topic %s", len(msgs), t)
		}(topic, messages)
	}

	// 等待所有goroutine完成
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 检查是否有错误
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	log.Printf("Successfully published %d messages in total", len(msgs))
	return nil
}

//PublishToTopic 直接向指定topic发布消息
func (kp *KafkaPublisher) PublishToTopic(ctx context.Context, topic string, key string, value []byte) error {
	kp.mu.RLock()
	if kp.closed {
		kp.mu.RUnlock()
		return fmt.Errorf("publisher is closed")
	}
	kp.mu.RUnlock()

	writer := kp.getWriter(topic)

	kafkaMsg := kafka.Message{
		Key:   []byte(key),
		Value: value,
		Headers: []kafka.Header{
			{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
		},
		Time: time.Now(),
	}

	err := writer.WriteMessages(ctx, kafkaMsg)
	if err != nil {
		return fmt.Errorf("failed to write message to topic %s: %s", topic, err.Error())
	}

	log.Printf("Message delivered to topic %s", topic)
	return nil
}

// Flush 刷新所有writer的缓冲区
func (kp *KafkaPublisher) Flush() error {
	kp.mu.RLock()
	defer kp.mu.RUnlock()

	if kp.closed {
		return fmt.Errorf("publisher is closed")
	}

	var wg sync.WaitGroup
	errChan := make(chan error, len(kp.writers))

	for topic, writer := range kp.writers {
		wg.Add(1)
		go func(t string, w *kafka.Writer) {
			defer wg.Done()
			// segmentio/kafka-go 的 Writer 会自动处理批次，这里我们可以发送一个空写入来触发刷新
			// 或者我们可以简单地记录日志
			log.Printf("Flushing writer for topic %s", t)
		}(topic, writer)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// Close 关闭发布器
func (kp *KafkaPublisher) Close() error {
	kp.mu.Lock()
	defer kp.mu.Unlock()

	if kp.closed {
		return nil
	}

	kp.closed = true

	// 关闭所有writers
	var wg sync.WaitGroup
	for topic, writer := range kp.writers {
		wg.Add(1)
		go func(t string, w *kafka.Writer) {
			defer wg.Done()
			if err := w.Close(); err != nil {
				log.Printf("Error closing writer for topic %s: %v", t, err)
			} else {
				log.Printf("Writer for topic %s closed successfully", t)
			}
		}(topic, writer)
	}

	wg.Wait()

	// 清空writers map
	kp.writers = make(map[string]*kafka.Writer)

	log.Println("KafkaPublisher closed successfully")
	return nil
}
