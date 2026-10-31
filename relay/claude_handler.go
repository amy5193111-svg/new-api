package relay

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/logger"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/model_setting"

	"github.com/gin-gonic/gin"
)

func ClaudeHelper(c *gin.Context, info *relaycommon.RelayInfo) (newAPIError *types.NewAPIError) {

	info.InitChannelMeta(c)

	claudeReq, ok := info.Request.(*dto.ClaudeRequest)

	if !ok {
		return types.NewErrorWithStatusCode(fmt.Errorf("invalid request type, expected *dto.ClaudeRequest, got %T", info.Request), types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
	}

	request, err := common.DeepCopy(claudeReq)
	if err != nil {
		return types.NewError(fmt.Errorf("failed to copy request to ClaudeRequest: %w", err), types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}

	err = helper.ModelMappedHelper(c, info, request)
	if err != nil {
		return types.NewError(err, types.ErrorCodeChannelModelMappedError, types.ErrOptionWithSkipRetry())
	}

	// 历史里若存在没有 tool_result 的 tool_use（客户端漏回传、重复 id 等），
	// 下游会直接 400（tool_use ids ... without tool_result），这里先补齐/去重。
	hardenClaudeToolPairs(request)
	if err := helper.ApplyReasoningModelSuffix(c, info, request); err != nil {
		return newConvertRequestFailedError(c, info, err)
	}

	adaptor := GetAdaptor(info.ApiType)
	if adaptor == nil {
		return types.NewError(fmt.Errorf("invalid api type: %d", info.ApiType), types.ErrorCodeInvalidApiType, types.ErrOptionWithSkipRetry())
	}
	adaptor.Init(info)

	if info.ChannelSetting.SystemPrompt != "" {
		if request.System == nil {
			request.SetStringSystem(info.ChannelSetting.SystemPrompt)
		} else if info.ChannelSetting.SystemPromptOverride {
			common.SetContextKey(c, constant.ContextKeySystemPromptOverride, true)
			if request.IsStringSystem() {
				existing := strings.TrimSpace(request.GetStringSystem())
				if existing == "" {
					request.SetStringSystem(info.ChannelSetting.SystemPrompt)
				} else {
					request.SetStringSystem(info.ChannelSetting.SystemPrompt + "\n" + existing)
				}
			} else {
				systemContents := request.ParseSystem()
				newSystem := dto.ClaudeMediaMessage{Type: dto.ContentTypeText}
				newSystem.SetText(info.ChannelSetting.SystemPrompt)
				if len(systemContents) == 0 {
					request.System = []dto.ClaudeMediaMessage{newSystem}
				} else {
					request.System = append([]dto.ClaudeMediaMessage{newSystem}, systemContents...)
				}
			}
		}
	}

	if !model_setting.GetGlobalSettings().PassThroughRequestEnabled &&
		!info.ChannelSetting.PassThroughBodyEnabled &&
		service.ShouldChatCompletionsUseResponsesGlobal(info.ChannelId, info.ChannelType, info.OriginModelName) {
		usage, newApiErr := textRequestViaResponses(c, info, adaptor, request)
		if newApiErr != nil {
			return newApiErr
		}

		service.PostTextConsumeQuota(c, info, usage, nil)
		return nil
	}

	var requestBody io.Reader
	if model_setting.GetGlobalSettings().PassThroughRequestEnabled || info.ChannelSetting.PassThroughBodyEnabled {
		storage, err := common.GetBodyStorage(c)
		if err != nil {
			return types.NewErrorWithStatusCode(err, types.ErrorCodeReadRequestBodyFailed, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
		}
		requestBody = common.NewReplayableBodyReader(storage)
	} else {
		convertedRequest, err := adaptor.ConvertClaudeRequest(c, info, request)
		if err != nil {
			return newConvertRequestFailedError(c, info, err)
		}
		relaycommon.AppendRequestConversionFromRequest(info, convertedRequest)
		jsonData, err := common.Marshal(convertedRequest)
		if err != nil {
			return types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}

		// remove disabled fields for Claude API
		jsonData, err = relaycommon.RemoveDisabledFields(jsonData, info.ChannelOtherSettings, info.ChannelSetting.PassThroughBodyEnabled)
		if err != nil {
			return types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}

		// apply param override
		if len(info.ParamOverride) > 0 {
			jsonData, err = relaycommon.ApplyParamOverrideWithRelayInfo(jsonData, info)
			if err != nil {
				return newAPIErrorFromParamOverride(err)
			}
		}

		logger.LogDebug(c, "requestBody: %s", jsonData)
		body, closer, err := relaycommon.NewOutboundJSONBody(jsonData)
		if err != nil {
			return types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
		}
		defer closer.Close()
		jsonData = nil
		requestBody = body
	}

	statusCodeMappingStr := c.GetString("status_code_mapping")
	var httpResp *http.Response
	resp, err := adaptor.DoRequest(c, info, requestBody)
	if err != nil {
		return types.NewOpenAIError(err, types.ErrorCodeDoRequestFailed, http.StatusInternalServerError)
	}

	if resp != nil {
		httpResp = resp.(*http.Response)
		info.IsStream = info.IsStream || strings.HasPrefix(httpResp.Header.Get("Content-Type"), "text/event-stream")
		if httpResp.StatusCode != http.StatusOK {
			newAPIError = service.RelayErrorHandler(c.Request.Context(), httpResp, false)
			// reset status code 重置状态码
			service.ResetStatusCode(newAPIError, statusCodeMappingStr)
			return newAPIError
		}
	}

	usage, newAPIError := adaptor.DoResponse(c, httpResp, info)
	if newAPIError != nil {
		// reset status code 重置状态码
		service.ResetStatusCode(newAPIError, statusCodeMappingStr)
		return newAPIError
	}

	service.PostTextConsumeQuota(c, info, usage.(*dto.Usage), nil)
	return nil
}

// hardenClaudeToolPairs 保证每条 assistant 消息里的 tool_use 在下一条 user 消息里
// 都有对应的 tool_result：缺失的补一条占位结果，重复 id 的只保留第一次出现。
// 目的：避免上游以 "tool_use ids were found without tool_result" 直接拒绝整轮请求。
func hardenClaudeToolPairs(req *dto.ClaudeRequest) {
	if req == nil || len(req.Messages) == 0 {
		return
	}
	msgs := req.Messages
	for i := 0; i < len(msgs); i++ {
		if msgs[i].Role != "assistant" {
			continue
		}
		blocks, ok := msgs[i].Content.([]any)
		if !ok || len(blocks) == 0 {
			continue
		}
		uses := make([]string, 0, len(blocks))
		seen := make(map[string]struct{}, len(blocks))
		kept := make([]any, 0, len(blocks))
		for _, raw := range blocks {
			b, isMap := raw.(map[string]any)
			if !isMap {
				kept = append(kept, raw)
				continue
			}
			t, _ := b["type"].(string)
			id, _ := b["id"].(string)
			// 只管我们自己造的调用 id（DSML 修复产物），
			// 真实 Claude 客户端的 toolu_ 类 id 一律不动。
			if t != "tool_use" || id == "" || !strings.HasPrefix(id, "call_dsml_") {
				kept = append(kept, raw)
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			uses = append(uses, id)
			kept = append(kept, raw)
		}
		if len(uses) == 0 {
			continue
		}
		msgs[i].Content = kept

		next := -1
		for j := i + 1; j < len(msgs); j++ {
			if msgs[j].Role == "user" {
				next = j
				break
			}
		}
		found := make(map[string]struct{}, len(uses))
		if next >= 0 {
			if nb, isArr := msgs[next].Content.([]any); isArr {
				for _, raw := range nb {
					b, isMap := raw.(map[string]any)
					if !isMap {
						continue
					}
					if t, _ := b["type"].(string); t != "tool_result" {
						continue
					}
					if id, _ := b["tool_use_id"].(string); id != "" {
						found[id] = struct{}{}
					}
				}
			}
		}
		missing := make([]string, 0, len(uses))
		for _, id := range uses {
			if _, ok := found[id]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) == 0 {
			continue
		}
		const placeholder = "[系统自动补齐] 上一次工具调用没有返回结果（客户端缺少 tool_result），已按空结果继续。"
		mk := func(id string) map[string]any {
			return map[string]any{"type": "tool_result", "tool_use_id": id, "content": placeholder}
		}
		if next < 0 {
			nb := make([]any, 0, len(missing))
			for _, id := range missing {
				nb = append(nb, mk(id))
			}
			req.Messages = append(req.Messages, dto.ClaudeMessage{Role: "user", Content: nb})
			continue
		}
		var nb []any
		if arr, isArr := msgs[next].Content.([]any); isArr {
			nb = arr
		}
		if s, isStr := msgs[next].Content.(string); isStr && s != "" {
			nb = append(nb, map[string]any{"type": "text", "text": s})
		}
		for _, id := range missing {
			nb = append(nb, mk(id))
		}
		msgs[next].Content = nb
	}
}
