package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ScanResult 扫描结果消息
type ScanResult struct {
	BaseMessage
	TaskID   string   `json:"task_id"`
	OpenPort []string `json:"open_ports"` // 扫描到的开放端口
}

// NewScanResult 创建扫描结果
func NewScanResult(id string) *ScanResult {
	return &ScanResult{
		BaseMessage: BaseMessage{
			ID:        id,
			Timestamp: time.Now(),
			Topic:     "scan-results",
		},
		TaskID:   fmt.Sprintf("scan-result:%s", id),
		OpenPort: []string{},
	}
}

func (s *ScanResult) GetKey() string {
	return s.TaskID
}

func (s *ScanResult) GetValue() []byte {
	data, _ := json.Marshal(s)
	return data
}

func (s *ScanResult) GetTopic() string {
	return s.Topic
}

func (s *ScanResult) Validate() error {
	if s.TaskID == "" {
		return errors.New("task_id is required")
	}
	return nil
}
