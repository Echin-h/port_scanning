package validator

import (
	"errors"
)

// ScanValidator 扫描任务验证器
type ScanValidator struct {
	ipValidator   *IPValidator
	portValidator *PortValidator
}

// NewScanValidator 创建扫描验证器
func NewScanValidator() *ScanValidator {
	return &ScanValidator{
		ipValidator:   NewIPValidator(),
		portValidator: NewPortValidator(),
	}
}

// ValidateIPs 验证IP
func (sv *ScanValidator) ValidateIPs(ips string) error {
	return sv.ipValidator.ValidateIPs(ips)
}

// ValidatePorts 验证端口
func (sv *ScanValidator) ValidatePorts(ports string) error {
	return sv.portValidator.ValidatePorts(ports)
}

// ValidateBandwidth 验证带宽
func (sv *ScanValidator) ValidateBandwidth(bandwidth int) error {
	if bandwidth <= 0 {
		return errors.New("bandwidth must be greater than 0")
	}
	if bandwidth > 10000 { // 限制最大带宽为 10Gbps
		return errors.New("bandwidth cannot exceed 10000 Mbps")
	}
	return nil
}

// ValidateTimeout 验证超时时间
func (sv *ScanValidator) ValidateTimeout(timeout int) error {
	if timeout <= 0 {
		return errors.New("timeout must be greater than 0")
	}
	if timeout > 3600 { // 限制最大超时时间为1小时
		return errors.New("timeout cannot exceed 3600 seconds")
	}
	return nil
}
