package relay_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type inputLimitRequest struct {
	dto.BaseRequest
	tokens int
}

func (r *inputLimitRequest) GetTokenCountMeta() *types.TokenCountMeta {
	return &types.TokenCountMeta{TokenType: types.TokenTypeTextNumber, CombineText: strings.Repeat("a", r.tokens), MaxTokens: 2000000}
}

func inputLimitSettings(t *testing.T) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	savedFree := operation_setting.GetQuotaSetting().EnableFreeModelPreConsume
	operation_setting.GetQuotaSetting().EnableFreeModelPreConsume = false
	saved := ratio_setting.ModelRatio2JSONString()
	require.NoError(t, ratio_setting.UpdateModelRatioByJSONString(`{"input-limit-test":0}`))
	savedCount := constant.CountToken
	constant.CountToken = false
	t.Cleanup(func() {
		operation_setting.GetQuotaSetting().EnableFreeModelPreConsume = savedFree
		require.NoError(t, ratio_setting.UpdateModelRatioByJSONString(saved))
		constant.CountToken = savedCount
	})
}

func inputLimitContext(id int, base string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	common.SetContextKey(c, constant.ContextKeyChannelId, id)
	common.SetContextKey(c, constant.ContextKeyChannelBaseUrl, base)
	common.SetContextKey(c, constant.ContextKeyOriginalModel, "input-limit-test")
	return c
}

func TestInputTokenLimitBeforeBilling(t *testing.T) {
	inputLimitSettings(t)
	c := inputLimitContext(19, "https://api.ermao.ink")
	info := &relaycommon.RelayInfo{Request: &inputLimitRequest{tokens: 1000001}, OriginModelName: "input-limit-test", UsingGroup: "default", RelayFormat: types.RelayFormatClaude}
	err := relay.PrepareRequestBilling(c, info)
	require.NotNil(t, err, "over-limit input must be rejected before billing even when CountToken is disabled")
	require.Equal(t, 400, err.StatusCode, "actual error: %v", err)
	require.Equal(t, relay.ErrorCodeInputTokenLimit, err.GetErrorCode())
	require.Nil(t, info.Billing)
	require.False(t, service.ShouldRetryRelayError(c, err, 3))
	require.False(t, service.ChargeViolationFeeIfNeeded(c, info, err))
}

func TestInputTokenLimitBoundariesAndSource(t *testing.T) {
	inputLimitSettings(t)
	for _, tc := range []struct {
		name       string
		id, tokens int
		base       string
		reject     bool
	}{
		{"below", 19, 999999, "https://api.ermao.ink", false},
		{"exact", 19, 1000000, "https://api.ermao.ink", false},
		{"above", 19, 1000001, "https://api.ermao.ink", true},
		{"channel56", 56, 1000001, "https://api.ermao.ink/v1", true},
		{"channel57", 57, 1000001, "https://API.ERMAO.INK:443/", true},
		{"channel63", 63, 1000001, "https://api.ermao.ink", true},
		{"different-channel", 20, 1000001, "https://api.ermao.ink", false},
		{"different-source", 19, 1000001, "https://other.example", false},
		{"host-suffix", 19, 1000001, "https://api.ermao.ink.evil.example", false},
		{"userinfo-spoof", 19, 1000001, "https://api.ermao.ink@other.example", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := inputLimitContext(tc.id, tc.base)
			info := &relaycommon.RelayInfo{Request: &inputLimitRequest{tokens: tc.tokens}, OriginModelName: "input-limit-test", UsingGroup: "default", RelayFormat: types.RelayFormatClaude}
			err := relay.PrepareRequestBilling(c, info)
			if tc.reject {
				require.NotNil(t, err)
				require.Equal(t, 400, err.StatusCode)
			} else {
				require.Nil(t, err, "output budget of 2000000 must not trigger the input cap")
			}
			require.Nil(t, info.Billing)
		})
	}
}

type inputLimitTransport struct {
	calls     int
	body      []byte
	failFirst bool
}

func (s *inputLimitTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	s.calls++
	var err error
	s.body, err = io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if s.failFirst && s.calls == 1 {
		return &http.Response{StatusCode: 503, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"isolated retry","type":"server_error"}}`)), Request: r}, nil
	}
	// No real upstream connection; only this network boundary is replaced.
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"msg_test","type":"message","role":"assistant","model":"input-limit-test","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`)), Request: r}, nil
}

func inputLimitDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(&model.User{}, &model.Token{}, &model.Channel{}, &model.Log{}, &model.Ability{}))
	oldDB, oldLog := model.DB, model.LOG_DB
	oldRedis, oldMemory := common.RedisEnabled, common.MemoryCacheEnabled
	oldMainType, oldLogType := common.MainDatabaseType(), common.LogDatabaseType()
	model.DB, model.LOG_DB = db, db
	common.RedisEnabled, common.MemoryCacheEnabled = false, false
	common.SetDatabaseTypes(common.DatabaseTypeSQLite, common.DatabaseTypeSQLite)
	oldMaster := common.IsMasterNode
	common.IsMasterNode = false
	t.Setenv("LOG_SQL_DSN", "")
	require.NoError(t, model.InitLogDB())
	require.NoError(t, i18n.Init())
	t.Cleanup(func() { common.IsMasterNode = oldMaster })
	t.Cleanup(func() {
		model.DB, model.LOG_DB = oldDB, oldLog
		common.RedisEnabled, common.MemoryCacheEnabled = oldRedis, oldMemory
		common.SetDatabaseTypes(oldMainType, oldLogType)
		require.NoError(t, sqlDB.Close())
	})
	require.NoError(t, db.Create(&model.User{Id: 4242, Username: "isolated-limit-test", Quota: 1000000, Group: "default"}).Error)
	require.NoError(t, db.Create(&model.Token{Id: 4242, UserId: 4242, Key: "isolated-limit-test", RemainQuota: 1000000}).Error)
	require.NoError(t, db.Create(&model.Channel{Id: 19, Name: "isolated-limit-test", Status: common.ChannelStatusEnabled, Type: constant.ChannelTypeAnthropic}).Error)
	return db
}

func TestInputTokenLimitRetryToRestrictedChannel(t *testing.T) {
	inputLimitSettings(t)
	db := inputLimitDB(t)
	oldRetry := common.RetryTimes
	common.RetryTimes = 1
	t.Cleanup(func() { common.RetryTimes = oldRetry })
	require.NoError(t, db.Model(&model.Channel{}).Where("id = ?", 19).Updates(map[string]any{"base_url": "https://api.ermao.ink", "key": "fake-key", "group": "default", "models": "input-limit-test"}).Error)
	require.NoError(t, db.Create(&model.Ability{ChannelId: 19, Group: "default", Model: "input-limit-test", Enabled: true}).Error)
	service.InitHttpClient()
	client, err := service.GetHttpClientWithProxySettings("", dto.ChannelSettings{})
	require.NoError(t, err)
	oldTransport := client.Transport
	t.Cleanup(func() { client.Transport = oldTransport })
	fake := &inputLimitTransport{failFirst: true}
	client.Transport = fake
	body, err := common.Marshal(map[string]any{"model": "input-limit-test", "max_tokens": 1, "messages": []any{map[string]any{"role": "user", "content": strings.Repeat("hello ", 1000001)}}})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(string(body)))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("channel_id", 20)
	c.Set("channel_type", constant.ChannelTypeAnthropic)
	c.Set("base_url", "https://other.example")
	c.Set("original_model", "input-limit-test")
	c.Set("group", "default")
	c.Set("using_group", "default")
	c.Set("token_group", "default")
	c.Set("id", 4242)
	c.Set("token_id", 4242)
	c.Set("token_key", "isolated-limit-test")
	c.Set("token_unlimited_quota", true)
	controller.Relay(c, types.RelayFormatClaude)
	require.Equal(t, 400, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, fake.calls, "retry must not forward oversized input to the newly selected restricted channel")
	require.Contains(t, recorder.Body.String(), "exceeds the maximum of 1000000")
	var user model.User
	require.NoError(t, db.First(&user, 4242).Error)
	require.Equal(t, 1000000, user.Quota)
	var channel model.Channel
	require.NoError(t, db.First(&channel, 19).Error)
	require.Equal(t, common.ChannelStatusEnabled, channel.Status)
}

func TestInputTokenLimitHTTPEntry(t *testing.T) {
	inputLimitSettings(t)
	db := inputLimitDB(t)
	service.InitHttpClient()
	client, err := service.GetHttpClientWithProxySettings("", dto.ChannelSettings{})
	require.NoError(t, err)
	oldTransport := client.Transport
	t.Cleanup(func() { client.Transport = oldTransport })
	for _, tc := range []struct {
		name, field string
		id          int
		base        string
		count       bool
		reject      bool
	}{
		{"normal-and-large-output", "normal", 19, "https://api.ermao.ink", false, false},
		{"over-input-count-off", "messages", 19, "https://api.ermao.ink", false, true},
		{"over-input-count-on", "messages", 19, "https://api.ermao.ink", true, true},
		{"system-cached", "system", 19, "https://api.ermao.ink", false, true},
		{"tools", "tools", 19, "https://api.ermao.ink", false, true},
		{"assistant-history", "history", 19, "https://api.ermao.ink", false, true},
		{"tool-result", "tool_result", 19, "https://api.ermao.ink", false, true},
		{"other-source-untruncated", "messages", 19, "https://other.example", false, false},
		{"other-channel-untruncated", "messages", 20, "https://api.ermao.ink", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			constant.CountToken = tc.count
			fake := &inputLimitTransport{}
			client.Transport = fake
			large := strings.Repeat("hello ", 1000001)
			request := map[string]any{"model": "input-limit-test", "max_tokens": 2000000, "messages": []any{map[string]any{"role": "user", "content": "hello"}}}
			switch tc.field {
			case "messages":
				request["messages"] = []any{map[string]any{"role": "user", "content": large}}
			case "system":
				request["system"] = []any{map[string]any{"type": "text", "text": large, "cache_control": map[string]any{"type": "ephemeral"}}}
			case "tools":
				request["tools"] = []any{map[string]any{"name": "lookup", "description": large, "input_schema": map[string]any{"type": "object"}}}
			case "history":
				request["messages"] = []any{map[string]any{"role": "assistant", "content": large}, map[string]any{"role": "user", "content": "continue"}}
			case "tool_result":
				request["messages"] = []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "test", "content": large}}}}
			}
			body, err := common.Marshal(request)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(string(body)))
			c.Request.Header.Set("Content-Type", "application/json")
			c.Set("channel_id", tc.id)
			c.Set("channel_type", constant.ChannelTypeAnthropic)
			c.Set("base_url", tc.base)
			c.Set("original_model", "input-limit-test")
			c.Set("group", "default")
			c.Set("using_group", "default")
			c.Set("id", 4242)
			c.Set("token_id", 4242)
			c.Set("token_key", "isolated-limit-test")
			c.Set("token_unlimited_quota", true)
			controller.Relay(c, types.RelayFormatClaude)
			if tc.reject {
				require.Equal(t, 400, recorder.Code, recorder.Body.String())
				require.Zero(t, fake.calls)
				require.Contains(t, recorder.Body.String(), "exceeds the maximum of 1000000")
				var user model.User
				require.NoError(t, db.First(&user, 4242).Error)
				require.Equal(t, 1000000, user.Quota)
				var token model.Token
				require.NoError(t, db.First(&token, 4242).Error)
				require.Equal(t, 1000000, token.RemainQuota)
				var channel model.Channel
				require.NoError(t, db.First(&channel, 19).Error)
				require.Equal(t, common.ChannelStatusEnabled, channel.Status)
			} else {
				require.Equal(t, 200, recorder.Code, recorder.Body.String())
				require.Equal(t, 1, fake.calls)
				var forwarded map[string]any
				require.NoError(t, common.Unmarshal(fake.body, &forwarded))
				require.EqualValues(t, 2000000, forwarded["max_tokens"])
				if tc.field == "messages" {
					require.Contains(t, string(fake.body), large, "unrelated sources must not be truncated")
				}
			}
		})
	}
}
