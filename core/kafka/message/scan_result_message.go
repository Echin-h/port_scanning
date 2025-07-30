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
	TaskID     string   `json:"task_id"`
	Status     string   `json:"status"` // open/closed/filtered
	Discovered []string `json:"discovered"`
	Total      int      `json:"total"` // 扫描结果总数
}

// NewScanResult 创建扫描结果
func NewScanResult(taskID, status string) *ScanResult {
	return &ScanResult{
		BaseMessage: BaseMessage{
			ID:        generateID(),
			Timestamp: time.Now(),
			Topic:     "scan-results",
		},
		TaskID:     taskID,
		Status:     status,
		Discovered: []string{},
		Total:      0,
	}
}

func (s *ScanResult) GetKey() string {
	return fmt.Sprintf("scan_result:%s", s.TaskID)
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
