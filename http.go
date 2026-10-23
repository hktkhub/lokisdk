package lokisdk

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// HTTP 层默认值与上限。
const (
	// DefaultHTTPTimeout 是 HTTP 客户端的默认请求超时。
	DefaultHTTPTimeout = 10 * time.Second
	// maxErrorBodySize 限制错误响应体的读取上限，避免异常响应撑大内存。
	maxErrorBodySize = 4 << 10
)

// HTTP 层哨兵错误。
var (
	// ErrHTTPConfig 表示 HTTP 客户端配置不合法。
	ErrHTTPConfig = errors.New("loki: invalid http config")
	// ErrHTTPStatus 表示服务端返回了非 2xx 状态码。
	ErrHTTPStatus = errors.New("loki: unexpected http status")
)

// HTTPConfig 是创建 HTTPClient 所需的配置。
type HTTPConfig struct {
	// BaseURL 是服务端根地址，需包含 scheme 与 host。
	BaseURL string
	// Timeout 是单次请求最大耗时；<= 0 时使用 DefaultHTTPTimeout。
	Timeout time.Duration
	// Username、Password 用于 HTTP Basic 认证。
	Username string
	Password string
	// BearerToken 用于 Bearer 认证，优先于 Basic 认证。
	BearerToken string
}

// HTTPClient 是面向 Loki 的请求发送器。
//
// 它复用一条带连接池的 http.Client，负责构造请求、注入认证、发送并校验响应。
// Client 通过组合持有一个 *HTTPClient 来发送日志，自身不直接接触 net/http。
type HTTPClient struct {
	baseURL     string
	username    string
	password    string
	bearerToken string
	client      *http.Client
}

// NewHTTPClient 依据 cfg 创建一个 HTTPClient。
//
// cfg.BaseURL 为空时返回 ErrHTTPConfig。
func NewHTTPClient(cfg HTTPConfig) (*HTTPClient, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("loki: http base url is required: %w", ErrHTTPConfig)
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultHTTPTimeout
	}

	return &HTTPClient{
		baseURL:     baseURL,
		username:    cfg.Username,
		password:    cfg.Password,
		bearerToken: cfg.BearerToken,
		client: &http.Client{
			Timeout:   timeout,
			Transport: newTransport(),
		},
	}, nil
}

// NewRequest 构造一个带认证头的请求；body 为 nil 时不携带请求体。
func (c *HTTPClient) NewRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("loki: build request %s %s: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	c.applyAuth(req)
	return req, nil
}

// Do 发送 req 并校验响应状态，非 2xx 返回 *HTTPStatusError。
//
// 成功时调用方负责关闭 resp.Body。
func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("loki: do request %s %s: %w", req.Method, req.URL.Path, err)
	}
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return resp, nil
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodySize))
	return nil, &HTTPStatusError{
		StatusCode: resp.StatusCode,
		Status:     resp.Status,
		Body:       strings.TrimSpace(string(body)),
	}
}

// applyAuth 按配置向 req 注入认证头，Bearer 优先于 Basic。
func (c *HTTPClient) applyAuth(req *http.Request) {
	switch {
	case c.bearerToken != "":
		req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	case c.username != "":
		req.SetBasicAuth(c.username, c.password)
	}
}

// HTTPStatusError 表示服务端返回了非 2xx 状态码。
type HTTPStatusError struct {
	StatusCode int
	Status     string
	Body       string
}

// Error 实现 error 接口。
func (e *HTTPStatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("loki: unexpected status %s", e.Status)
	}
	return fmt.Sprintf("loki: unexpected status %s: %s", e.Status, e.Body)
}

// Unwrap 让 errors.Is(err, ErrHTTPStatus) 成立。
func (e *HTTPStatusError) Unwrap() error { return ErrHTTPStatus }

// newTransport 返回一个复用 DefaultTransport 连接池参数的 Transport，
// 仅覆盖日志推送关心的握手超时与保活间隔。
func newTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.TLSHandshakeTimeout = 5 * time.Second
	transport.ResponseHeaderTimeout = 5 * time.Second
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	return transport
}
