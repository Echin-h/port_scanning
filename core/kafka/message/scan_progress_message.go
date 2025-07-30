package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ScanProgress 扫描进度消息
type ScanProgress struct {
	BaseMessage
	TaskID  string        `json:"task_id"`
	Message string        `json:"message"` // 状态描述
	Status  MessageStatus `json:"status"`  // running/completed/failed/cancelled
}

// NewScanProgress 创建扫描进度
func NewScanProgress(taskID, message string, status MessageStatus) *ScanProgress {
	return &ScanProgress{BaseMessage: BaseMessage{
		ID:        generateID(),
		Timestamp: time.Now(),
		Topic:     "scan-progress",
	},
		TaskID:  taskID,
		Status:  status,
		Message: message,
	}
}

func (s *ScanProgress) GetKey() string {
	return fmt.Sprintf("scan_progress:%s", s.TaskID)
}

func (s *ScanProgress) GetValue() []byte {
	data, _ := json.Marshal(s)
	return data
}

func (s *ScanProgress) GetTopic() string {
	return s.Topic
}

func (s *ScanProgress) Validate() error {
	if s.TaskID == "" {
		return errors.New("task_id is required")
	}
	return nil
}
