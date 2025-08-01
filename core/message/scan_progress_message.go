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
	TaskID string `json:"task_id"`

	Rate      float64  // kpps
	Progress  float64  // 百分比
	Remaining string   // 剩余时间
	Found     int      // 发现的端口数
	OpenPorts []string // 发现的开放端口
}

// NewScanProgress 创建扫描进度
func NewScanProgress(taskID string) *ScanProgress {
	return &ScanProgress{BaseMessage: BaseMessage{
		ID:        generateID(),
		Timestamp: time.Now(),
		Topic:     "scan-progress",
	},
		TaskID: taskID,
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
