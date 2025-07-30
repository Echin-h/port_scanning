package validator

import (
	"fmt"
	"net"
	"strings"
)

// IPValidator IP验证器
type IPValidator struct{}

// NewIPValidator 创建IP验证器
func NewIPValidator() *IPValidator {
	return &IPValidator{}
}

// ValidateIPs 验证IP字符串（支持单个IP、CIDR、IP范围、多个IP）
func (v *IPValidator) ValidateIPs(ips string) error {
	if ips == "" {
		return fmt.Errorf("ips cannot be empty")
	}

	ips = strings.TrimSpace(ips)

	// 检查是否是CIDR格式
	if strings.Contains(ips, "/") {
		return v.validateCIDR(ips)
	}

	// 检查是否是IP范围格式 (如: 192.168.1.1-192.168.1.10)
	if strings.Contains(ips, "-") {
		return v.validateIPRange(ips)
	}

	// 检查是否是逗号分隔的多个IP
	if strings.Contains(ips, ",") {
		return v.validateMultipleIPs(ips)
	}

	// 单个IP
	return v.validateSingleIP(ips)
}

// validateCIDR 验证CIDR格式
func (v *IPValidator) validateCIDR(cidr string) error {
	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return fmt.Errorf("invalid CIDR format: %s", cidr)
	}

	// 检查网络大小限制
	ones, bits := ipNet.Mask.Size()
	if bits-ones > 16 { // 限制最大65536个IP
		return fmt.Errorf("CIDR network too large (max /16): %s", cidr)
	}

	return nil
}

// validateIPRange 验证IP范围
func (v *IPValidator) validateIPRange(rangeStr string) error {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid IP range format: %s", rangeStr)
	}

	startIP := net.ParseIP(strings.TrimSpace(parts[0]))
	endIP := net.ParseIP(strings.TrimSpace(parts[1]))

	if startIP == nil {
		return fmt.Errorf("invalid start IP: %s", strings.TrimSpace(parts[0]))
	}
	if endIP == nil {
		return fmt.Errorf("invalid end IP: %s", strings.TrimSpace(parts[1]))
	}

	// 转换为IPv4
	startIPv4 := startIP.To4()
	endIPv4 := endIP.To4()

	if startIPv4 == nil || endIPv4 == nil {
		return fmt.Errorf("only IPv4 ranges are supported: %s", rangeStr)
	}

	// 检查IP范围是否合理
	if compareIPs(startIPv4, endIPv4) > 0 {
		return fmt.Errorf("start IP is greater than end IP: %s", rangeStr)
	}

	// 检查范围大小限制
	if v.calculateIPRangeSize(startIPv4, endIPv4) > 65536 {
		return fmt.Errorf("IP range too large (max 65536 IPs): %s", rangeStr)
	}

	return nil
}

// validateMultipleIPs 验证多个IP
func (v *IPValidator) validateMultipleIPs(ips string) error {
	ipList := strings.Split(ips, ",")

	if len(ipList) > 1000 { // 限制最大1000个IP
		return fmt.Errorf("too many IPs (max 1000): %d", len(ipList))
	}

	for i, ip := range ipList {
		ip = strings.TrimSpace(ip)
		if err := v.validateSingleIP(ip); err != nil {
			return fmt.Errorf("invalid IP at position %d: %w", i+1, err)
		}
	}

	return nil
}

// validateSingleIP 验证单个IP
func (v *IPValidator) validateSingleIP(ip string) error {
	if net.ParseIP(ip) == nil {
		return fmt.Errorf("invalid IP address: %s", ip)
	}
	return nil
}

// calculateIPRangeSize 计算IP范围大小
func (v *IPValidator) calculateIPRangeSize(startIP, endIP net.IP) int {
	start := ipToUint32(startIP)
	end := ipToUint32(endIP)
	return int(end - start + 1)
}

// 辅助函数
func compareIPs(ip1, ip2 net.IP) int {
	if ip1 == nil || ip2 == nil {
		return 0
	}

	for i := 0; i < len(ip1) && i < len(ip2); i++ {
		if ip1[i] < ip2[i] {
			return -1
		}
		if ip1[i] > ip2[i] {
			return 1
		}
	}
	return 0
}

func ipToUint32(ip net.IP) uint32 {
	ip = ip.To4()
	return uint32(ip[0])<<24 + uint32(ip[1])<<16 + uint32(ip[2])<<8 + uint32(ip[3])
}
