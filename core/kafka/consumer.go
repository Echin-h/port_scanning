package kafka

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"port_scanning/core/kafka/config"
)

// CallbackFunc 回调函数类型定义
type CallbackFunc func(context.Context, *kafka.Message) error

// KafkaConsumer Kafka接收器实现
type KafkaConsumer struct {
	readers   map[string]*kafka.Reader // 每个topic一个reader
	callbacks map[string]CallbackFunc  // 每个topic的回调函数
	config    *config.KafkaConfig
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closed    bool
	mu        sync.RWMutex
}

// NewKafkaConsumer 创建Kafka接收器
func NewKafkaConsumer() (*KafkaConsumer, error) {
	cfg := config.GetKafkaConfig()
	ctx, cancel := context.WithCancel(context.Background())

	kr := &KafkaConsumer{
		readers:   make(map[string]*kafka.Reader),
		callbacks: make(map[string]CallbackFunc),
		config:    cfg,
		ctx:       ctx,
		cancel:    cancel,
		closed:    false,
	}
	return kr, nil
}

// RegisterCallback 注册回调函数
func (kr *KafkaConsumer) RegisterCallback(topic string, callback CallbackFunc) error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	// 检查接收器是否已关闭
	if kr.closed {
		return errors.New("Consumer is closed")
	}

	// 如果存在则替换
	if _, exists := kr.callbacks[topic]; exists {
		kr.callbacks[topic] = callback
		return nil
	}

	// 创建reader
	reader := kr.config.GetReaderForTopic(topic)
	kr.readers[topic] = reader
	kr.callbacks[topic] = callback

	return nil
}

// Start 启动接收器
func (kr *KafkaConsumer) Start() error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return errors.New("consumer is closed")
	}

	if len(kr.callbacks) == 0 {
		return errors.New("no callbacks registered for any topics")
	}

	// 为每个topic启动消费协程
	for topic, callback := range kr.callbacks {
		reader := kr.readers[topic]
		kr.wg.Add(1)
		go kr.consumeMessages(topic, reader, callback)
	}

	return nil
}

// consumeMessages 消费指定topic的消息
func (kr *KafkaConsumer) consumeMessages(topic string, reader *kafka.Reader, callback CallbackFunc) {
	defer kr.wg.Done()
	log.Printf("Started consuming messages from topic: %s", topic)

	for {
		select {
		case <-kr.ctx.Done():
			log.Printf("Stopping consumer for topic: %s", topic)
			return
		default:
			// 设置读取超时
			//ctx, cancel := context.WithTimeout(kr.ctx, 10*time.Second)
			// todo: 设置超时时间
			// 全部读出来，然后放到一个channel中，去异步处理
			ctx, cancel := context.WithTimeout(kr.ctx, 5*time.Second)
			// fetching message: context deadline exceeded
			//log.Println("我正在消费消息，超时时间是5秒钟")
			msg, err := reader.ReadMessage(ctx)
			cancel()

			if err != nil {
				// 超时是正常的，继续循环
				if errors.Is(err, context.DeadlineExceeded) {
					log.Println("读取消息超时，继续循环")
					continue
				}
				// 上下文被取消，退出
				if errors.Is(err, context.Canceled) {
					return
				}
				// 短暂休眠后重试
				log.Printf("Error reading message from topic %s: %v", topic, err)
				time.Sleep(time.Second)
				continue
			}

			// 处理消息
			if err := kr.handleMessage(topic, &msg, callback); err != nil {
				log.Printf("Error handling message from topic %s: %v", topic, err)
			}
		}
	}
}

// handleMessage 处理单个消息
func (kr *KafkaConsumer) handleMessage(topic string, msg *kafka.Message, callback CallbackFunc) error {
	// 创建处理上下文，设置超时
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 记录消息接收
	log.Printf("Consumed message from topic %s, partition %d, offset %d",
		topic, msg.Partition, msg.Offset)

	// 调用回调函数
	startTime := time.Now()
	err := callback(ctx, msg)
	duration := time.Since(startTime)

	if err != nil {
		log.Printf("Callback failed for topic %s (took %v): %v", topic, duration, err)
		return err
	}

	log.Printf("Message processed successfully for topic %s (took %v)", topic, duration)
	return nil
}

// ConsumeWithCallback 动态添加topic消费（不需要预先注册）
func (kr *KafkaConsumer) ConsumeWithCallback(topic string, callback CallbackFunc) error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return errors.New("consumer is closed")
	}

	// 检查是否已经存在
	if _, exists := kr.callbacks[topic]; exists {
		return errors.New("callback for topic already exists")
	}

	// 创建reader
	reader := kr.config.GetReaderForTopic(topic)
	kr.readers[topic] = reader
	kr.callbacks[topic] = callback

	// 立即启动消费协程
	kr.wg.Add(1)
	go kr.consumeMessages(topic, reader, callback)

	log.Printf("Started consuming messages from topic: %s with callback", topic)
	return nil
}

// RemoveCallback 移除指定topic的回调函数
func (kr *KafkaConsumer) RemoveCallback(topic string) error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return errors.New("consumer is closed")
	}

	reader, exists := kr.readers[topic]
	if !exists {
		return errors.New("no reader found for topic")
	}

	// 关闭reader（这会导致消费协程退出）
	if err := reader.Close(); err != nil {
		log.Printf("Error closing reader for topic %s: %v", topic, err)
	}

	// 清理映射
	delete(kr.readers, topic)
	delete(kr.callbacks, topic)

	log.Printf("Removed callback for topic: %s", topic)
	return nil
}

// GetStats 获取接收器统计信息
func (kr *KafkaConsumer) GetStats() map[string]interface{} {
	kr.mu.RLock()
	defer kr.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["closed"] = kr.closed
	stats["readers_count"] = len(kr.readers)
	stats["callbacks_count"] = len(kr.callbacks)

	topics := make([]string, 0, len(kr.readers))
	for topic := range kr.readers {
		topics = append(topics, topic)
	}
	stats["topics"] = topics

	return stats
}

// Stop 停止接收器
func (kr *KafkaConsumer) Stop() error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return nil
	}

	log.Println("Stopping KafkaConsumer...")

	// 取消上下文，停止所有消费协程
	kr.cancel()

	// 等待所有协程结束
	kr.wg.Wait()

	// 关闭所有readers
	for topic, reader := range kr.readers {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing reader for topic %s: %v", topic, err)
		} else {
			log.Printf("Reader for topic %s closed successfully", topic)
		}
	}

	kr.closed = true
	kr.readers = make(map[string]*kafka.Reader)
	kr.callbacks = make(map[string]CallbackFunc)

	log.Println("KafkaConsumer stopped successfully")
	return nil
}
