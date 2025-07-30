package message

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"port_scanning/core/validator"
)

// ScanTask 扫描任务消息
type ScanTask struct {
	BaseMessage
	TaskID    string `json:"task_id"`
	IPs       string `json:"ips"`       // IP列表  可以为单个IP,CIDR格式
	Ports     string `json:"ports"`     // 端口列表  可以为单个端口,端口范围(如: 80-100),逗号分隔的多个端口
	Bandwidth int    `json:"bandwidth"` // 扫描带宽 (KB/s) // Mbps
	Timeout   int    `json:"timeout"`   // 超时时间(秒)
}

// NewScanTask 创建扫描任务
func NewScanTask(ips, ports string, bandwidth, wait int) *ScanTask {
	return &ScanTask{
		BaseMessage: BaseMessage{
			ID:        generateID(),
			Timestamp: time.Now(),
			Topic:     "scan-tasks",
		},
		TaskID:    generateTaskID(),
		IPs:       ips,       // 127.0.0.1
		Ports:     ports,     // --ports 80,443
		Bandwidth: bandwidth, // --bandwidth 100
		Timeout:   wait,      // --wait 5s
	}
}

func (s *ScanTask) GetKey() string {
	return s.TaskID
}

func (s *ScanTask) GetValue() []byte {
	data, _ := json.Marshal(s)
	return data
}

func (s *ScanTask) GetTopic() string {
	return s.Topic
}

func (s *ScanTask) Validate() error {
	// 创建验证器
	v := validator.NewScanValidator()

	// 验证 TaskID
	if s.TaskID == "" {
		return errors.New("task_id is required")
	}

	// 验证 IPs
	if s.IPs == "" {
		return errors.New("ips is required")
	}
	if err := v.ValidateIPs(s.IPs); err != nil {
		log.Println("IP validation error:", err)
		return err
	}

	// 验证 Ports
	if s.Ports == "" {
		return errors.New("ports is required")
	}
	if err := v.ValidatePorts(s.Ports); err != nil {
		log.Println("Port validation error:", err)
		return err
	}

	// 验证 Bandwidth
	if err := v.ValidateBandwidth(s.Bandwidth); err != nil {
		log.Println("Bandwidth validation error:", err)
		return err
	}

	// 验证 Timeout
	if err := v.ValidateTimeout(s.Timeout); err != nil {
		log.Println("Timeout validation error:", err)
		return err
	}

	return nil
}

// GetTotalTargets 获取总扫描目标数
func (s *ScanTask) GetTotalTargets() int {
	return len(s.IPs) * len(s.Ports)
}
