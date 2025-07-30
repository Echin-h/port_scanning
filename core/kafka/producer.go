package kafka

// TODO: 每次发布一个就需要NewKafkaProducer，这样会导致每次都创建新的writer，可能会导致资源浪费

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"port_scanning/core/kafka/config"
	"port_scanning/core/kafka/message"
)

// KafkaProducer Kafka发布器实现
type KafkaProducer struct {
	writers map[string]*kafka.Writer // 每个topic一个writer
	config  *config.KafkaConfig
	//partitioner Partitioner // 分区器
	closed bool
	mu     sync.RWMutex
}

// NewKafkaProducer 创建Kafka发布器
func NewKafkaProducer() (*KafkaProducer, error) {
	cfg := config.GetKafkaConfig()

	kp := &KafkaProducer{
		writers: make(map[string]*kafka.Writer),
		//partitioner: NewIPSegmentPartitioner(24), // 默认分区器
		config: cfg,
		closed: false,
	}

	return kp, nil
}

func Produce(message message.Message) error {
	producer, err := NewKafkaProducer()
	if err != nil {
		return err
	}

	defer producer.Close() // 确保资源被释放

	ctx := context.Background()
	if err := producer.Produce(ctx, message); err != nil {
		log.Printf("Failed to produce message: %v", err)
		return err
	}

	return nil
}

// getWriter 获取或创建指定topic的writer
func (kp *KafkaProducer) getWriter(topic string) *kafka.Writer {
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

// Produce 同步发布消息
func (kp *KafkaProducer) Produce(ctx context.Context, msg message.Message) error {
	return kp.ProduceToTopic(ctx, msg.GetTopic(), msg.GetKey(), msg.GetValue())
}

// ProduceAsync 异步发布消息
func (kp *KafkaProducer) ProduceAsync(ctx context.Context, msg message.Message, callback func(error)) {
	go func() {
		err := kp.Produce(ctx, msg)
		if callback != nil {
			callback(err)
		}
	}()
}

// ProduceBatch 批量发布消息
func (kp *KafkaProducer) ProduceBatch(ctx context.Context, msgs []message.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	kp.mu.RLock()
	if kp.closed {
		kp.mu.RUnlock()
		return errors.New("producer is closed")
	}
	kp.mu.RUnlock()

	// 验证所有消息
	for _, msg := range msgs {
		if err := msg.Validate(); err != nil {
			log.Println("Message validation failed:", err)
			return err
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
			log.Printf("Successfully Produceed %d messages to topic %s", len(msgs), t)
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

	log.Printf("Successfully Produceed %d messages in total", len(msgs))
	return nil
}

// ProduceToTopic 直接向指定topic发布消息
func (kp *KafkaProducer) ProduceToTopic(ctx context.Context, topic string, key string, value []byte) error {
	kp.mu.RLock()
	if kp.closed {
		kp.mu.RUnlock()
		return errors.New("producer is closed")
	}
	kp.mu.RUnlock()

	// 获取分区
	//partition, err := kp.partitioner.GetPartition(key, 2) // 硬编码为2个分区
	//if err != nil {
	//	log.Printf("Warning: failed to calculate partition for key %s, using partition 0: %v", key, err)
	//	partition = 0
	//}
	//log.Println("Using partition:", partition)

	writer := kp.getWriter(topic)
	kafkaMsg := kafka.Message{
		Key:   []byte(key),
		Value: value,
		//Partition: partition,
		Headers: []kafka.Header{
			{Key: "timestamp", Value: []byte(time.Now().Format(time.RFC3339))},
			//{Key: "partition", Value: []byte(fmt.Sprintf("%d", partition))},
		},
		Time: time.Now(),
	}

	err := writer.WriteMessages(ctx, kafkaMsg)
	if err != nil {
		log.Println("Failed to write message to topic:", topic, "Error:", err)
		return err
	}

	log.Printf("Message delivered to topic %s", topic)
	return nil
}

// Flush 刷新所有writer的缓冲区
func (kp *KafkaProducer) Flush() error {
	kp.mu.RLock()
	defer kp.mu.RUnlock()

	if kp.closed {
		return errors.New("producer is closed")
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
func (kp *KafkaProducer) Close() error {
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

	log.Println("KafkaProducer closed successfully")
	return nil
}
