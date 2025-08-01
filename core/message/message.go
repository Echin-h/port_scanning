package message

import (
	"fmt"
	"time"
)

// Message 消息接口
type Message interface {
	GetKey() string
	GetValue() []byte
	GetTopic() string
	Validate() error
}

// BaseMessage 基础消息结构
type BaseMessage struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Topic     string    `json:"topic"`
}

// generateID 生成消息ID
func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
