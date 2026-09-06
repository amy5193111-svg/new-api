package controller

import (
	"strings"

	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/system_setting"
)

func paymentReturnPath(suffix string) string {
	// 优先使用自定义回调地址（外网可访问），否则回退到服务器地址
	base := strings.TrimRight(operation_setting.CustomCallbackAddress, "/")
	if base == "" {
		base = strings.TrimRight(system_setting.ServerAddress, "/")
	}
	return base + suffix
}
