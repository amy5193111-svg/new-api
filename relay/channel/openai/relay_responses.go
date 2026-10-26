package openai

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

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func OaiResponsesHandler(c *gin.Context, info *relaycommon.RelayInfo, resp *http.Response) (*dto.Usage, *types.NewAPIError) {
	defer service.CloseResponseBodyGracefully(resp)

	// read response body
	var responsesResponse dto.OpenAIResponsesResponse
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, types.NewOpenAIError(err, types.ErrorCodeReadResponseBodyFailed, http.StatusInternalServerError)
	}
	err = common.Unmarshal(responseBody, &responsesResponse)
	if err != nil {
		return nil, types.NewOpenAIError(err, types.ErrorCodeBadResponseBody, http.StatusInternalServerError)
	}
	if oaiError := responsesResponse.GetOpenAIError(); oaiError != nil && oaiError.Type != "" {
		return nil, types.WithOpenAIError(*oaiError, resp.StatusCode)
	}

	info.ObserveResponseModel(responsesResponse.Model)
	responseBody = rewriteSGLangResponsesCreatedAt(info, responseBody, "created_at", responsesResponse.CreatedAt)

	// 写入新的 response body
	service.IOCopyBytesGracefully(c, resp, responseBody)

	// compute usage
	usage := &dto.Usage{}
	service.ApplyResponsesUsage(usage, responsesResponse.Usage)
	// Count actual tool invocations from Output (not tool declarations).
	for _, output := range responsesResponse.Output {
		switch output.Type {
		case dto.BuildInCallWebSearchCall:
			info.CountBillableToolCall(dto.BuildInCallWebSearchCall, "")
		case dto.BuildInCallFileSearchCall:
			info.CountBillableToolCall(dto.BuildInCallFileSearchCall, "")
		case dto.BuildInCallFunctionCall:
			info.CountBillableToolCall(dto.BuildInCallFunctionCall, output.Name)
		}
	}
	info.ApplyVendorToolUsage(responseBody)

	imageCounter := &relaycommon.ImageGenerationCallCounter{}
	for i := range responsesResponse.Output {
		imageCounter.Observe(&responsesResponse.Output[i], &i)
	}
	imageCounter.Commit(info)

	return usage, nil
}

func OaiResponsesStreamHandler(c *gin.Context, info *relaycommon.RelayInfo, resp *http.Response) (*dto.Usage, *types.NewAPIError) {
	if resp == nil || resp.Body == nil {
		logger.LogError(c, common.LogText("invalid response or response body"))
		return nil, types.NewError(fmt.Errorf("invalid response"), types.ErrorCodeBadResponse)
	}

	defer service.CloseResponseBodyGracefully(resp)

	accumulator := service.NewResponsesUsageAccumulator(info)
	normalizer := newResponsesStreamNormalizer()

	helper.StreamScannerHandler(c, resp, info, func(data string, sr *helper.StreamResult) {

		// 检查当前数据是否包含 completed 状态和 usage 信息
		var streamResponse dto.ResponsesStreamResponse
		if err := common.UnmarshalJsonStr(data, &streamResponse); err != nil {
			logger.LogError(c, common.LogText("failed to unmarshal stream response: %s", err.Error()))
			sr.Error(err)
			return
		}
		if streamResponse.Response != nil {
			data = string(rewriteSGLangResponsesCreatedAt(info, []byte(data), "response.created_at", streamResponse.Response.CreatedAt))
		}
		patched, forward := normalizer.handle([]byte(data), &streamResponse)
		if !forward {
			return
		}
		data = string(patched)
		sendResponsesStreamData(c, streamResponse, data)
		accumulator.Observe(&streamResponse, common.StringToByteSlice(data))
	})

	common.SetContextKey(c, constant.ContextKeyResponseStreamStatus, info.StreamStatus)
	info.StreamStatus.RequireTerminal()
	return accumulator.Finish(), nil
}

// 透传层的 Responses 流一致性兜底。
//
// 问题一：部分上游（如 direct.wjapi.vip 系列）在流式阶段通告的 output item id
// 与终态 response.output[i].id 不是同一个值。OpenAI 规范要求同一 item 在
// output_item.added/done 与终态里 identity 一致，严格实现的客户端（Cortico、
// 部分 Codex 封装）会直接判定整条响应无效，表现为「一调工具就失败、纯聊天正常」。
//
// 问题二：部分上游（如 direct.hongai.co）会把同一个 content part 通告两遍，
// 同样会触发客户端的 "Duplicate content part" 校验。
//
// 这里只在透传路径上做最小修正：记住每个 output_index 首次通告的 item id，
// 回填 output_item.done 与终态；重复的 content part 通告直接丢弃。
// 上游本身规范时，这里全部是空操作。
type responsesStreamNormalizer struct {
	announcedItemID map[int]string
	partSeen        map[string]struct{}
	partDeduped     map[string]struct{}
}

func newResponsesStreamNormalizer() *responsesStreamNormalizer {
	return &responsesStreamNormalizer{
		announcedItemID: make(map[int]string),
		partSeen:        make(map[string]struct{}),
		partDeduped:     make(map[string]struct{}),
	}
}

// handle 返回修正后的负载，以及该事件是否应转发给客户端。
func (n *responsesStreamNormalizer) handle(payload []byte, s *dto.ResponsesStreamResponse) ([]byte, bool) {
	if n == nil || s == nil {
		return payload, true
	}

	switch s.Type {
	case "response.output_item.added":
		if s.Item == nil || s.OutputIndex == nil {
			return payload, true
		}
		id := strings.TrimSpace(s.Item.ID)
		if id == "" {
			return payload, true
		}
		prev, seen := n.announcedItemID[*s.OutputIndex]
		if seen {
			if prev == id {
				// 同一个 item 被通告两次，丢掉后一次
				return payload, false
			}
			return payload, true
		}
		n.announcedItemID[*s.OutputIndex] = id
		return payload, true

	case "response.output_item.done":
		if s.Item == nil || s.OutputIndex == nil {
			return payload, true
		}
		want, ok := n.announcedItemID[*s.OutputIndex]
		got := strings.TrimSpace(s.Item.ID)
		if !ok || want == "" || got == "" || want == got {
			return payload, true
		}
		patched, err := sjson.SetBytes(payload, "item.id", want)
		if err != nil {
			return payload, true
		}
		s.Item.ID = want
		return patched, true

	case "response.content_part.added":
		key := responsesContentPartKey(s)
		if key == "" {
			return payload, true
		}
		if _, dup := n.partSeen[key]; dup {
			n.partDeduped[key] = struct{}{}
			return payload, false
		}
		n.partSeen[key] = struct{}{}
		return payload, true

	case "response.content_part.done":
		key := responsesContentPartKey(s)
		if key == "" {
			return payload, true
		}
		if _, dropped := n.partDeduped[key]; dropped {
			return payload, false
		}
		return payload, true

	case "response.completed", "response.done", "response.incomplete", "response.failed":
		if s.Response == nil {
			return payload, true
		}
		for index := range s.Response.Output {
			want := n.announcedItemID[index]
			got := strings.TrimSpace(s.Response.Output[index].ID)
			if want == "" || got == "" || want == got {
				continue
			}
			patched, err := sjson.SetBytes(payload, fmt.Sprintf("response.output.%d.id", index), want)
			if err != nil {
				continue
			}
			payload = patched
			s.Response.Output[index].ID = want
		}
		return payload, true
	}

	return payload, true
}

func responsesContentPartKey(s *dto.ResponsesStreamResponse) string {
	itemID := strings.TrimSpace(s.ItemID)
	if itemID == "" && s.Item != nil {
		itemID = strings.TrimSpace(s.Item.ID)
	}
	if itemID == "" {
		return ""
	}
	index := 0
	if s.ContentIndex != nil {
		index = *s.ContentIndex
	}
	return fmt.Sprintf("%s#%d", itemID, index)
}

func rewriteSGLangResponsesCreatedAt(info *relaycommon.RelayInfo, payload []byte, path string, createdAt dto.IntValue) []byte {
	if info.GetChannelType() != constant.ChannelTypeSGLang {
		return payload
	}
	if !gjson.GetBytes(payload, path).Exists() {
		return payload
	}
	patched, err := sjson.SetBytes(payload, path, int(createdAt))
	if err != nil {
		return payload
	}
	return patched
}
