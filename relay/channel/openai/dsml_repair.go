package openai

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relay/helper"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"

	"github.com/gin-gonic/gin"
)

// dsml_repair: 把上游偶尔降级成纯文本的 DSML 工具调用块还原成标准 tool_calls。
//
// 线上真实泄漏字节（从用户会话历史里取出的原文，非推测）：
//   '<' U+FF5C "DSML" U+FF5C ' ' "calls>"
//   '<' U+FF5C "DSML" U+FF5C ' ' 'invoke name="run_code">'
//   '<' U+FF5C "DSML" U+FF5C ' ' 'parameter name="code" string="true">' ... 值 ... '</' U+FF5C "DSML" U+FF5C "parameter>"
//   '</' U+FF5C "DSML" U+FF5C "invoke>"
//   '</' U+FF5C "DSML" U+FF5C "calls>"
//
// 设计要点：默认零改动透传；只有遇到标记才局部缓存。正文尾部最多暂留
// dsmlMaxTail 字节用于判断是否半个标记，因此首字延迟不受影响。
const (
	dsmlPipe     = "\uff5c"
	dsmlMaxTail  = 16
	dsmlMaxBlock = 1 << 20
)

var dsmlHeads = []string{
	"<" + dsmlPipe + "DSML" + dsmlPipe,
	"</" + dsmlPipe + "DSML" + dsmlPipe,
	"<|DSML|",
	"</|DSML|",
}

type dsmlOp struct {
	text  string
	calls []dto.ToolCallResponse
}


type dsmlState struct {
	buf      string
	inBlock  bool
	sawBlock bool
}

type dsmlRewriter struct {
	cs      dsmlState
	rs      dsmlState
	toolSeq int
	id      string
	model   string
	created int64

	countOn bool
	blocks  int
	failed  int
	tools   []string
	sample  string
	near       int
	nearSample string
}

func newDSMLRewriter(countOn bool) *dsmlRewriter {
	return &dsmlRewriter{countOn: countOn}
}

func (r *dsmlRewriter) markFailed(b string) {
	if !r.countOn {
		return
	}
	r.failed++
	if r.sample == "" {
		s := b
		if len(s) > 800 {
			s = s[:800]
		}
		r.sample = strings.ReplaceAll(strings.ReplaceAll(s, "\n", " "), "\r", " ")
	}
}

func dsmlStrPtr(s string) *string { return &s }

func (r *dsmlRewriter) emit(c *gin.Context, delta dto.ChatCompletionsStreamResponseChoiceDelta, finish *string) {
	resp := dto.ChatCompletionsStreamResponse{
		Id:      r.id,
		Object:  "chat.completion.chunk",
		Created: r.created,
		Model:   r.model,
		Choices: []dto.ChatCompletionsStreamResponseChoice{
			{Index: 0, Delta: delta, FinishReason: finish},
		},
	}
	if err := helper.ObjectData(c, resp); err != nil {
		common.SysLog("dsml_repair: write chunk failed: " + err.Error())
	}
}

// Feed 处理一个 SSE 数据块。无标记时走快路径，原样透传，字节级不变。
func (r *dsmlRewriter) Feed(c *gin.Context, data string) error {
	if strings.TrimSpace(data) == "" {
		return nil
	}
	var chunk dto.ChatCompletionsStreamResponse
	if err := common.UnmarshalJsonStr(data, &chunk); err != nil {
		helper.StringData(c, data)
		return nil
	}
	if r.id == "" {
		r.id = chunk.Id
		r.created = chunk.Created
		r.model = chunk.Model
	}

	content := ""
	reasoning := ""
	finish := ""
	for i := range chunk.Choices {
		ch := chunk.Choices[i]
		if ch.Delta.Content != nil {
			content += *ch.Delta.Content
		}
		if ch.Delta.ReasoningContent != nil {
			reasoning += *ch.Delta.ReasoningContent
		}
		if ch.FinishReason != nil && *ch.FinishReason != "" {
			finish = *ch.FinishReason
		}
	}

	// 快路径：无挂起缓冲、无标记字符、无需改 finish_reason -> 原样透传
	if r.cs.buf == "" && !r.cs.inBlock && r.rs.buf == "" && !r.rs.inBlock &&
		!strings.ContainsRune(content, '<') && !strings.ContainsRune(reasoning, '<') &&
		!strings.Contains(content, "DSML") && !strings.Contains(reasoning, "DSML") &&
		!((r.cs.sawBlock || r.rs.sawBlock) && finish != "") {
		helper.StringData(c, data)
		return nil
	}

	b0, f0 := r.blocks, r.failed
	if reasoning != "" {
		for _, op := range r.flow(&r.rs, reasoning) {
			if op.text != "" {
				r.emit(c, dto.ChatCompletionsStreamResponseChoiceDelta{ReasoningContent: dsmlStrPtr(op.text)}, nil)
			}
			if len(op.calls) > 0 {
				r.emit(c, dto.ChatCompletionsStreamResponseChoiceDelta{ToolCalls: op.calls}, nil)
			}
		}
	}
	if content != "" {
		for _, op := range r.flow(&r.cs, content) {
			if op.text != "" {
				r.emit(c, dto.ChatCompletionsStreamResponseChoiceDelta{Content: dsmlStrPtr(op.text)}, nil)
			}
			if len(op.calls) > 0 {
				r.emit(c, dto.ChatCompletionsStreamResponseChoiceDelta{ToolCalls: op.calls}, nil)
			}
		}
	}
	for i := range chunk.Choices {
		if len(chunk.Choices[i].Delta.ToolCalls) > 0 {
			r.emit(c, dto.ChatCompletionsStreamResponseChoiceDelta{ToolCalls: chunk.Choices[i].Delta.ToolCalls}, nil)
		}
	}
	// 近似命中：文本里出现 DSML 字样但没被状态机识别（形态不同/半角圆括号被替换等），
	// 只留痕不处理，用来定位上游到底吐的是什么形态。
	if r.countOn && r.blocks == b0 && r.failed == f0 &&
		(strings.Contains(content, "DSML") || strings.Contains(reasoning, "DSML")) {
		if r.nearSample == "" {
			all := content
			if all == "" {
				all = reasoning
			}
			seg := all
			if i := strings.Index(seg, "DSML"); i > 60 {
				seg = seg[i-60:]
			}
			if len(seg) > 120 {
				seg = seg[:120]
			}
			r.nearSample = strings.ReplaceAll(strings.ReplaceAll(seg, "\n", " "), "\r", " ")
		}
		if r.near < 9 {
			r.near++
		}
	}
	if finish != "" {
		f := finish
		if (r.cs.sawBlock || r.rs.sawBlock) && f == "stop" {
			f = "tool_calls"
		}
		r.emit(c, dto.ChatCompletionsStreamResponseChoiceDelta{}, dsmlStrPtr(f))
	}
	return nil
}

func (r *dsmlRewriter) Finish(c *gin.Context, info *relaycommon.RelayInfo) {
	// 未闭合的块一律丢弃，不再原样吐给客户端（拦截外显）；只记 failed + 样本。
	if r.cs.buf != "" {
		if r.cs.inBlock {
			r.markFailed(r.cs.buf)
		} else {
			r.emit(c, dto.ChatCompletionsStreamResponseChoiceDelta{Content: dsmlStrPtr(r.cs.buf)}, nil)
		}
		r.cs = dsmlState{}
	}
	if r.rs.buf != "" {
		if r.rs.inBlock {
			r.markFailed(r.rs.buf)
		} else {
			r.emit(c, dto.ChatCompletionsStreamResponseChoiceDelta{ReasoningContent: dsmlStrPtr(r.rs.buf)}, nil)
		}
		r.rs = dsmlState{}
	}
	if !r.countOn || (r.blocks == 0 && r.failed == 0 && r.near == 0) {
		return
	}
	ch, model, up := 0, "", ""
	if info != nil {
		ch = info.ChannelId
		model = info.OriginModelName
		up = info.UpstreamModelName
	}
	sample := ""
	if r.sample != "" {
		sample = fmt.Sprintf(" sample=%q", r.sample)
	}
	near := ""
	if r.near > 0 {
		near = fmt.Sprintf(" near=%d nearSample=%q", r.near, r.nearSample)
	}
	common.SysLog(fmt.Sprintf("dsml_repair hit ch=%d model=%s upstream=%s blocks=%d failed=%d tools=%s%s%s",
		ch, model, up, r.blocks, r.failed, strings.Join(r.tools, ","), sample, near))
}

func (r *dsmlRewriter) flow(st *dsmlState, text string) []dsmlOp {
	var ops []dsmlOp
	st.buf += text
	for {
		if !st.inBlock {
			idx, closing, body, end, ok := dsmlNext(st.buf, 0)
			if !ok {
				cut := dsmlSafeCut(st.buf)
				if cut > 0 {
					ops = appendDsmlText(ops, st.buf[:cut])
					st.buf = st.buf[cut:]
				}
				return ops
			}
			if !closing && strings.HasPrefix(body, "calls") {
				if idx > 0 {
					ops = appendDsmlText(ops, st.buf[:idx])
				}
				st.buf = st.buf[idx:]
				st.inBlock = true
				continue
			}
			if idx > 0 {
				ops = appendDsmlText(ops, st.buf[:idx])
			}
			st.buf = st.buf[end:]
			continue
		}

		_, ce, ok := dsmlFindClose(st.buf, 0, "calls")
		if ok {
			block := st.buf[:ce]
			st.buf = st.buf[ce:]
			st.inBlock = false
			st.sawBlock = true
			if calls := parseDsmlBlock(block); len(calls) > 0 {
				ops = append(ops, dsmlOp{calls: calls})
				if r.countOn {
					r.blocks++
					for _, cl := range calls {
						if len(r.tools) < 5 {
							r.tools = append(r.tools, cl.Function.Name)
						}
					}
				}
			} else {
				ops = appendDsmlText(ops, block)
				r.markFailed(block)
			}
			continue
		}
		if len(st.buf) > dsmlMaxBlock {
			ops = appendDsmlText(ops, st.buf)
			r.markFailed(st.buf)
			st.buf = ""
			st.inBlock = false
		}
		return ops
	}
}

func appendDsmlText(ops []dsmlOp, t string) []dsmlOp {
	if t == "" {
		return ops
	}
	if n := len(ops); n > 0 && len(ops[n-1].calls) == 0 {
		ops[n-1].text += t
		return ops
	}
	return append(ops, dsmlOp{text: t})
}

func parseDsmlBlock(block string) []dto.ToolCallResponse {
	var out []dto.ToolCallResponse
	pos := 0
	for {
		_, closing, body, end, ok := dsmlNext(block, pos)
		if !ok {
			break
		}
		pos = end
		if closing || !strings.HasPrefix(body, "invoke") {
			continue
		}
		name := dsmlAttr(body, "name")
		if name == "" {
			continue
		}
		args := map[string]any{}
		for {
			_, cl2, b2, e2, ok2 := dsmlNext(block, pos)
			if !ok2 {
				break
			}
			pos = e2
			if cl2 {
				if strings.HasPrefix(b2, "invoke") {
					break
				}
				continue
			}
			if !strings.HasPrefix(b2, "parameter") {
				continue
			}
			pname := dsmlAttr(b2, "name")
			if pname == "" {
				continue
			}
			cs, ce, ok3 := dsmlFindClose(block, pos, "parameter")
			if !ok3 {
				break
			}
			val := block[pos:cs]
			pos = ce
			if dsmlAttr(b2, "string") == "false" {
				var v any
				if err := common.UnmarshalJsonStr(strings.TrimSpace(val), &v); err == nil {
					args[pname] = v
				} else {
					args[pname] = val
				}
			} else {
				args[pname] = val
			}
		}
		b, err := json.Marshal(args)
		if err != nil {
			b = []byte("{}")
		}
		idx := len(out)
		out = append(out, dto.ToolCallResponse{
			Index: &idx,
			ID:    fmt.Sprintf("call_dsml_%d_%d", time.Now().UnixNano(), idx),
			Type:  "function",
			Function: dto.FunctionResponse{
				Name:      name,
				Arguments: string(b),
			},
		})
	}
	return out
}

// dsmlNext ????? from ?????????????????
func dsmlNext(s string, from int) (idx int, closing bool, body string, end int, ok bool) {
	pipe := dsmlPipe + "DSML" + dsmlPipe
	alt := "|DSML|"
	for i := from; i < len(s); i++ {
		if s[i] != '<' {
			continue
		}
		j := i + 1
		isClose := false
		if j < len(s) && s[j] == '/' {
			isClose = true
			j++
		}
		after := -1
		if strings.HasPrefix(s[j:], pipe) {
			after = j + len(pipe)
		} else if strings.HasPrefix(s[j:], alt) {
			after = j + len(alt)
		}
		if after < 0 {
			continue
		}
		gt := strings.IndexByte(s[after:], '>')
		if gt < 0 {
			return 0, false, "", 0, false
		}
		return i, isClose, strings.TrimSpace(s[after : after+gt]), after + gt + 1, true
	}
	return 0, false, "", 0, false
}

func dsmlFindClose(s string, from int, name string) (start int, end int, ok bool) {
	pos := from
	for {
		idx, closing, body, e, ok2 := dsmlNext(s, pos)
		if !ok2 {
			return 0, 0, false
		}
		if closing && strings.HasPrefix(body, name) {
			return idx, e, true
		}
		pos = e
	}
}

func dsmlAttr(body, key string) string {
	k := key + "=\""
	i := strings.Index(body, k)
	if i < 0 {
		return ""
	}
	j := i + len(k)
	e := strings.IndexByte(body[j:], '"')
	if e < 0 {
		return ""
	}
	return body[j : j+e]
}

// dsmlSafeCut ?????????????????????????? DSLM ??
func dsmlSafeCut(s string) int {
	limit := len(s) - dsmlMaxTail
	if limit < 0 {
		limit = 0
	}
	for i := len(s) - 1; i >= limit; i-- {
		if s[i] != '<' {
			continue
		}
		tail := s[i:]
		for _, h := range dsmlHeads {
			if len(tail) < len(h) && strings.HasPrefix(h, tail) {
				return i
			}
			if strings.HasPrefix(tail, h) && !strings.ContainsRune(tail, '>') {
				return i
			}
		}
	}
	return len(s)
}

// stripDsmlBlocks ??????????? DSML ?????? reasoning_content ???
func stripDsmlBlocks(s string) (string, bool) {
	if !strings.Contains(s, "DSML") {
		return s, false
	}
	var out strings.Builder
	pos := 0
	removed := false
	for {
		idx, closing, body, end, ok := dsmlNext(s, pos)
		if !ok {
			break
		}
		out.WriteString(s[pos:idx])
		if !closing && strings.HasPrefix(body, "calls") {
			_, ce, ok2 := dsmlFindClose(s, end, "calls")
			if !ok2 {
				out.WriteString(s[idx:end])
				pos = end
				continue
			}
			pos = ce
			removed = true
			continue
		}
		pos = end
		removed = true
	}
	out.WriteString(s[pos:])
	return out.String(), removed
}

// streamEmit ?? SSE ??????? dsml_repair ????????????
func streamEmit(c *gin.Context, info *relaycommon.RelayInfo, rw *dsmlRewriter, data string) error {
	if rw != nil {
		return rw.Feed(c, data)
	}
	return HandleStreamFormat(c, info, data, info.ChannelSetting.ForceFormat, info.ChannelSetting.ThinkingToContent)
}
