package lokisdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// pushPath 是 Loki 写入日志的接口路径。
const pushPath = "/loki/api/v1/push"

// entry 是一条待发送的日志记录。
//
// labels 是该条日志的 stream 标签集，包含 level 以及按需动态加入的其它维度；
// line 是序列化后的日志正文；基础标签 app/service/env 在发送时统一合并。
type entry struct {
	time   time.Time
	labels []Label
	line   string
}

// lokiPushRequest 是 Loki push 接口的请求体。
type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

// lokiStream 是一组同标签的日志序列。
type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

// push 把一批 entry 按完整标签集分组成 stream 并推送到 Loki。
func (c *Client) push(ctx context.Context, entries []entry) error {
	if len(entries) == 0 {
		return nil
	}

	payload, err := c.encodePush(entries)
	if err != nil {
		return err
	}

	req, err := c.http.NewRequest(ctx, http.MethodPost, pushPath, payload)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// encodePush 将 entries 按完整标签集分组并序列化为 Loki push 请求体。
func (c *Client) encodePush(entries []entry) ([]byte, error) {
	grouped := make(map[string]*lokiStream)
	order := make([]string, 0, 8)
	for _, e := range entries {
		labels := c.streamLabels(e.labels)
		key := labelSetKey(labels)
		s, ok := grouped[key]
		if !ok {
			s = &lokiStream{Stream: labels}
			grouped[key] = s
			order = append(order, key)
		}
		s.Values = append(s.Values, [2]string{
			strconv.FormatInt(e.time.UnixNano(), 10),
			e.line,
		})
	}

	streams := make([]lokiStream, 0, len(order))
	for _, key := range order {
		streams = append(streams, *grouped[key])
	}

	payload, err := json.Marshal(lokiPushRequest{Streams: streams})
	if err != nil {
		return nil, fmt.Errorf("loki: marshal push request: %w", err)
	}
	return payload, nil
}

// streamLabels 合并基础标签与 entry 的标签，返回完整的 stream 标签集。
func (c *Client) streamLabels(entryLabels []Label) map[string]string {
	labels := map[string]string{
		string(App):     c.labels.App,
		string(Service): c.labels.Service,
		string(Env):     c.labels.Env,
	}
	for _, l := range entryLabels {
		if l.Value == "" {
			continue
		}
		labels[string(l.Key)] = l.Value
	}
	return labels
}

// labelSetKey 把标签集编码成稳定的分组键，保证同标签的 entry 落到同一 stream。
func labelSetKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		v := labels[k]
		b.WriteString(strconv.Itoa(len(k)))
		b.WriteByte(':')
		b.WriteString(k)
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteByte(':')
		b.WriteString(v)
	}
	return b.String()
}