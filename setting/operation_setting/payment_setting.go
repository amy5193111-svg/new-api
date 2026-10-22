package operation_setting

import (
	"github.com/QuantumNous/new-api/setting/config"
	"math"
	"strconv"
)

type PaymentSetting struct {
	AmountOptions  []float64          `json:"amount_options"`
	AmountDiscount map[string]float64 `json:"amount_discount"` // 充值金额对应的折扣，例如 100 元 0.9 表示 100 元充值享受 9 折优惠

	ComplianceConfirmed    bool   `json:"compliance_confirmed"`
	ComplianceTermsVersion string `json:"compliance_terms_version"`
	ComplianceConfirmedAt  int64  `json:"compliance_confirmed_at"`
	ComplianceConfirmedBy  int    `json:"compliance_confirmed_by"`
	ComplianceConfirmedIP  string `json:"compliance_confirmed_ip"`
}

const CurrentComplianceTermsVersion = "v1"

// 默认配置
var paymentSetting = PaymentSetting{
	AmountOptions:  []float64{10, 20, 50, 100, 200, 500},
	AmountDiscount: map[string]float64{},
}

func init() {
	// 注册到全局配置管理器
	config.GlobalConfig.Register("payment_setting", &paymentSetting)
}

func GetPaymentSetting() *PaymentSetting {
	return &paymentSetting
}

func GetAmountDiscount(amount float64) float64 {
	const tolerance = 0.01
	bestDistance := tolerance + 1
	bestDiscount := 1.0
	for rawAmount, discount := range paymentSetting.AmountDiscount {
		presetAmount, err := strconv.ParseFloat(rawAmount, 64)
		if err != nil || discount <= 0 {
			continue
		}
		distance := math.Abs(amount - presetAmount)
		if distance <= tolerance && distance < bestDistance {
			bestDistance = distance
			bestDiscount = discount
		}
	}
	return bestDiscount
}

func IsPaymentComplianceConfirmed() bool {
	return paymentSetting.ComplianceConfirmed &&
		paymentSetting.ComplianceTermsVersion == CurrentComplianceTermsVersion
}
