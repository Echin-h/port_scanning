// message/message.go
package message

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message 消息接口
type Message interface {
	GetKey() string
	GetValue() []byte
	GetTopic() string
	Validate() error
}

// MessageHandler 消息处理器接口
type MessageHandler interface {
	Handle(ctx context.Context, msg *kafka.Message) error
	GetTopic() string
}

// BaseMessage 基础消息结构
type BaseMessage struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Topic     string    `json:"-"`
}

// generateID 生成消息ID
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// GenerateScanTaskID 生成扫描任务ID
func generateTaskID() string {
	return fmt.Sprintf("scan-task-%d", time.Now().UnixNano())
}
