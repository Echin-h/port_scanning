package validator

import (
	"fmt"
	"strconv"
	"strings"
)

// PortValidator 端口验证器
type PortValidator struct{}

// NewPortValidator 创建端口验证器
func NewPortValidator() *PortValidator {
	return &PortValidator{}
}

// ValidatePorts 验证端口字符串（支持单个端口、端口范围、多个端口）
func (v *PortValidator) ValidatePorts(ports string) error {
	if ports == "" {
		return fmt.Errorf("ports cannot be empty")
	}

	ports = strings.TrimSpace(ports)

	// 分割逗号分隔的端口
	portParts := strings.Split(ports, ",")

	totalPorts := 0
	for i, part := range portParts {
		part = strings.TrimSpace(part)

		var portCount int
		var err error

		// 检查是否是端口范围格式 (如: 80-100)
		if strings.Contains(part, "-") {
			portCount, err = v.validatePortRange(part)
		} else {
			// 单个端口
			err = v.validateSinglePort(part)
			portCount = 1
		}

		if err != nil {
			return fmt.Errorf("invalid port at position %d: %w", i+1, err)
		}

		totalPorts += portCount
	}

	// 限制总端口数量
	if totalPorts > 10000 {
		return fmt.Errorf("too many ports (max 10000): %d", totalPorts)
	}

	return nil
}

// validatePortRange 验证端口范围
func (v *PortValidator) validatePortRange(rangeStr string) (int, error) {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid port range format: %s", rangeStr)
	}

	startPort, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, fmt.Errorf("invalid start port: %s", parts[0])
	}

	endPort, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, fmt.Errorf("invalid end port: %s", parts[1])
	}

	if err := v.validatePortNumber(startPort); err != nil {
		return 0, fmt.Errorf("start port: %w", err)
	}

	if err := v.validatePortNumber(endPort); err != nil {
		return 0, fmt.Errorf("end port: %w", err)
	}

	if startPort > endPort {
		return 0, fmt.Errorf("start port is greater than end port: %d-%d", startPort, endPort)
	}

	portCount := endPort - startPort + 1

	// 限制单个范围的端口数量
	if portCount > 5000 {
		return 0, fmt.Errorf("port range too large (max 5000 ports): %d-%d", startPort, endPort)
	}

	return portCount, nil
}

// validateSinglePort 验证单个端口
func (v *PortValidator) validateSinglePort(portStr string) error {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port number: %s", portStr)
	}

	return v.validatePortNumber(port)
}

// validatePortNumber 验证端口号范围
func (v *PortValidator) validatePortNumber(port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("port out of range (1-65535): %d", port)
	}
	return nil
}
