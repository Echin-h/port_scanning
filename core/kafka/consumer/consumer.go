package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// CallbackFunc 回调函数类型定义
type CallbackFunc func(context.Context, *kafka.Message) error

// Consumer Kafka接收器实现
type Consumer struct {
	readers   map[string]*kafka.Reader // 每个topic一个reader
	callbacks map[string]CallbackFunc  // 每个topic的回调函数

	config *ConsumerConfig

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
	mu sync.RWMutex

	closed bool

	ch          chan MessageTask
	workerCount int

	consumedCount  int64
	processedCount int64
	errorCount     int64
}

type MessageTask struct {
	Topic    string
	Message  *kafka.Message
	Callback CallbackFunc
}

// NewConsumer 创建, 这里的cfg不能为空
func NewConsumer(cfg *ConsumerConfig, asyncCfg *AsyncConfig) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	if asyncCfg == nil {
		asyncCfg = &AsyncConfig{
			WorkerCount: 10,
			QueueSize:   1000,
		}
	}

	kr := &Consumer{
		readers:   make(map[string]*kafka.Reader),
		callbacks: make(map[string]CallbackFunc),
		config:    cfg,
		ctx:       ctx,
		cancel:    cancel,
		closed:    false,

		ch:          make(chan MessageTask, asyncCfg.QueueSize), // 默认为1000个任务
		workerCount: asyncCfg.WorkerCount,                       // 默认为5个worker

		consumedCount:  0,
		processedCount: 0,
		errorCount:     0,
	}

	return kr
}

// RegisterCallback 注册回调函数
func (kr *Consumer) RegisterCallback(topic string, callback CallbackFunc) error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	// 检查接收器是否已关闭
	if kr.closed {
		return errors.New("consumer is closed")
	}

	// 如果存在则替换
	if _, exists := kr.callbacks[topic]; exists {
		kr.callbacks[topic] = callback
		return nil
	}

	// 创建reader
	reader := kr.config.CreateReaderForTopic(topic)
	kr.readers[topic] = reader
	kr.callbacks[topic] = callback

	return nil
}

// Start 凹凸曼开始打怪
func (kr *Consumer) Start() error {
	kr.mu.Lock()
	defer kr.mu.Unlock()

	if kr.closed {
		return errors.New("consumer is closed")
	}

	if len(kr.callbacks) == 0 {
		return errors.New("no callbacks registered for any topics")
	}

	// 为每个topic启动消费协程
	for topic := range kr.callbacks {
		kr.wg.Add(1)
		go kr.consumeMessages(topic)
	}

	for i := 0; i < kr.workerCount; i++ {
		kr.wg.Add(1)
		go kr.worker(i)
	}

	return nil
}

// worker 工作协程 - 处理消息
func (kr *Consumer) worker(workerID int) {
	defer kr.wg.Done()

	for {
		select {
		// when consumer context cancel return
		case <-kr.ctx.Done():
			log.Println("Worker", workerID, "Stopping")
			return
		case task, ok := <-kr.ch:
			if !ok { // 如果channel已关闭，退出
				return
			}

			// todo: 如果执行失败应该怎么做?
			if err := kr.handleMessage(kr.ctx, task.Message, task.Callback); err != nil {
				atomic.AddInt64(&kr.errorCount, 1)
				log.Printf("Worker %d error processing message from topic %s: %v", workerID, task.Topic, err)
				continue
			}

			atomic.AddInt64(&kr.processedCount, 1)
		}
	}
}

// consumeMessages 消费消息 - 一直阻塞等待，快速消费
func (kr *Consumer) consumeMessages(topic string) {
	defer kr.wg.Done()

	reader := kr.readers[topic]
	callback := kr.callbacks[topic]

	for {
		// 一直阻塞等待消息，这是正常的
		fmt.Println("等待消息中.....")
		msg, err := reader.ReadMessage(kr.ctx)
		if err != nil {
			// 如果是context取消，直接退出
			if errors.Is(err, context.Canceled) {
				log.Printf("Consumer for topic %s stopped due to context cancellation", topic)
				return
			}

			log.Printf("Error reading message from topic %s: %v", topic, err)
			time.Sleep(time.Millisecond * 100) // 避免疯狂重试
			continue
		}

		log.Println("有msg出来辣:", string(msg.Value))
		log.Println("消息Offset:", msg.Offset, "分区:", msg.Partition)

		// 快速将消息放入处理队列
		task := MessageTask{
			Topic:    topic,
			Message:  &msg,
			Callback: callback,
		}

		// todo: 队列满了怎么办 & 处理速度跟不上怎么办？
		select {
		case kr.ch <- task:
			atomic.AddInt64(&kr.consumedCount, 1)
		case <-kr.ctx.Done():
			return
		}
	}
}

// ConsumeWithCallback 动态添加topic消费（不需要预先注册）
func (kr *Consumer) ConsumeWithCallback(topic string, callback CallbackFunc) error {
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
	reader := kr.config.CreateReaderForTopic(topic)
	kr.readers[topic] = reader
	kr.callbacks[topic] = callback

	// 立即启动消费协程
	kr.wg.Add(1)
	go kr.consumeMessages(topic)

	log.Printf("Started consuming messages from topic: %s with callback", topic)
	return nil
}

// handleMessage 处理单个消息
func (kr *Consumer) handleMessage(ctx context.Context, msg *kafka.Message, callback CallbackFunc) error {
	return callback(ctx, msg)
}

// RemoveCallback 移除指定topic的回调函数
func (kr *Consumer) RemoveCallback(topic string) error {
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

// Stop 停止consumer
func (kr *Consumer) Stop() error {
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

	// 关闭channel
	close(kr.ch)
	// 关闭所有readers
	for topic, reader := range kr.readers {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing reader for topic %s: %v", topic, err)
		} else {
			log.Printf("Reader for topic %s closed successfully", topic)
		}
	}

	kr.readers = make(map[string]*kafka.Reader)
	kr.callbacks = make(map[string]CallbackFunc)

	kr.closed = true

	log.Println("Kafka consumer stopped successfully")
	return nil
}

// todo: 能不能展示进度哇
func (kr *Consumer) processView() {}
