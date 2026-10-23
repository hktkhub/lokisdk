package lokisdk

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// 默认配置值。
const (
	// DefaultTimeout 是单次请求的默认超时时间。
	DefaultTimeout = 10 * time.Second
	// DefaultBatchSize 是批量发送的默认条数阈值。
	DefaultBatchSize = 200
	// DefaultFlushInterval 是默认的定时推送间隔。
	DefaultFlushInterval = 500 * time.Millisecond
	// DefaultMaxBatchBytes 是单批（单请求）体积上限，达到即发送（256 KiB）。
	DefaultMaxBatchBytes = 256 << 10
	// DefaultMaxBufferBytes 是内存缓冲上限，取单批上限的 10 倍（2.5 MiB），满则丢弃。
	DefaultMaxBufferBytes = 10 * DefaultMaxBatchBytes
)

// ErrInvalidConfig 表示传入的客户端配置不合法。
var ErrInvalidConfig = errors.New("loki: invalid config")

// BaseLabels 是必填的基础 stream 标签，调用方必须显式提供。
type BaseLabels struct {
	App     string
	Service string
	Env     string
}

// options 是 Client 的可选配置容器，字段私有，只能通过 Option 修改。
type options struct {
	// timeout 是单次请求超时。
	timeout time.Duration
	// username、password 用于 Basic 认证。
	username string
	password string
	// bearerToken 用于 Bearer 认证。
	bearerToken string
	// batchSize 是攒够多少条日志触发发送。
	batchSize int
	// flushInterval 是定时推送频率。
	flushInterval time.Duration
	// maxBatchBytes 是单批（单请求）体积上限，达到即发送。
	maxBatchBytes int64
	// maxBufferBytes 是内存缓冲字节上限，满时丢弃数据。
	maxBufferBytes int64
	// minLevel 是最低输出级别，低于该级别的日志会被直接丢弃。
	minLevel LogLevel
}

// defaultOptions 返回所有可选配置的默认值。
func defaultOptions() options {
	return options{
		timeout:        DefaultTimeout,
		batchSize:      DefaultBatchSize,
		flushInterval:  DefaultFlushInterval,
		maxBatchBytes:  DefaultMaxBatchBytes,
		maxBufferBytes: DefaultMaxBufferBytes,
		minLevel:       LevelDebug,
	}
}

// Client 是 Loki HTTP API 的客户端。
type Client struct {
	labels   BaseLabels
	http     *HTTPClient
	disp     *dispatcher
	minLevel LogLevel
}

var defaultSender *Client

// NewClient 创建一个 Loki Client。
//
// endpoint 与 base 为必填项：endpoint 需包含 scheme 与 host，base 的三个 label
// 都不能为空，否则返回 ErrInvalidConfig。其余可选配置通过 opts 注入，未提供的项使用默认值。
func NewClient(endpoint string, base BaseLabels, opts ...Option) (*Client, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("loki: endpoint is required: %w", ErrInvalidConfig)
	}
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("loki: parse endpoint %q: %w: %w", baseURL, err, ErrInvalidConfig)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("loki: endpoint %q must include scheme and host: %w", baseURL, ErrInvalidConfig)
	}
	if strings.TrimSpace(base.App) == "" || strings.TrimSpace(base.Service) == "" || strings.TrimSpace(base.Env) == "" {
		return nil, fmt.Errorf("loki: app, service and env labels are required: %w", ErrInvalidConfig)
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if strings.TrimSpace(o.username) != "" && strings.TrimSpace(o.bearerToken) != "" {
		return nil, fmt.Errorf("loki: basic auth and bearer token are mutually exclusive: %w", ErrInvalidConfig)
	}

	httpClient, err := NewHTTPClient(HTTPConfig{
		BaseURL:     baseURL,
		Timeout:     o.timeout,
		Username:    o.username,
		Password:    o.password,
		BearerToken: o.bearerToken,
	})
	if err != nil {
		return nil, fmt.Errorf("loki: init http client: %w", err)
	}

	c := &Client{
		labels:   base,
		http:     httpClient,
		minLevel: o.minLevel,
	}
	c.disp = newDispatcher(o.batchSize, o.flushInterval, o.maxBatchBytes, o.maxBufferBytes, c.push)

	return c, nil
}

// Run 将本 Client 装配为包级日志函数的默认发送端，并启动后台发送循环。
//
// 一个进程内只应调用一次；重复调用会启动多个后台循环。
func (c *Client) Run() {
	defaultSender = c
	c.disp.start()
}

// Flush 立即发送缓冲区中尚未发出的日志。
func (c *Client) Flush(ctx context.Context) error {
	if err := c.disp.send(ctx); err != nil {
		return fmt.Errorf("loki: flush: %w", err)
	}
	return nil
}

// Close 停止后台发送循环并做最后一次刷新，用于优雅停机。
//
// ctx 用于限定等待后台循环退出的时间；超时返回包装后的 ctx.Err()。
func (c *Client) Close(ctx context.Context) error {
	if err := c.disp.close(ctx); err != nil {
		return fmt.Errorf("loki: close: %w", err)
	}
	return nil
}

// Option 是创建 Client 时的修饰器，用于按需覆盖可选配置。
//
// 传入非法值（例如非正数、空串、nil）时该项保持默认值，不会返回错误。
type Option func(*options)

// WithTimeout 设置单次请求的最大耗时；d <= 0 时忽略。
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// WithBasicAuth 设置 HTTP Basic 认证凭据，与 WithBearerToken 互斥；均为空时忽略。
func WithBasicAuth(username, password string) Option {
	return func(o *options) {
		if strings.TrimSpace(username) != "" || strings.TrimSpace(password) != "" {
			o.username = username
			o.password = password
		}
	}
}

// WithBearerToken 设置 Bearer 认证令牌，与 WithBasicAuth 互斥；token 为空时忽略。
func WithBearerToken(token string) Option {
	return func(o *options) {
		if strings.TrimSpace(token) != "" {
			o.bearerToken = token
		}
	}
}

// WithBatchSize 设置批量发送的条数阈值；n <= 0 时忽略。
func WithBatchSize(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.batchSize = n
		}
	}
}

// WithMaxBatchBytes 设置单批（单请求）体积上限，达到即发送；n <= 0 时忽略。
func WithMaxBatchBytes(n int64) Option {
	return func(o *options) {
		if n > 0 {
			o.maxBatchBytes = n
		}
	}
}

// WithFlushInterval 设置定时推送间隔；d <= 0 时忽略。
func WithFlushInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.flushInterval = d
		}
	}
}

// WithMaxBufferBytes 设置内存缓冲字节上限，满时丢弃数据；n <= 0 时忽略。
func WithMaxBufferBytes(n int64) Option {
	return func(o *options) {
		if n > 0 {
			o.maxBufferBytes = n
		}
	}
}

// WithMinLevel 设置最低输出级别，低于该级别的日志会被直接丢弃；level 非法时忽略。
//
// 级别由低到高为 debug < info < warn < error。默认 LevelDebug，即不限制。
func WithMinLevel(level LogLevel) Option {
	return func(o *options) {
		if levelSeverity(level) >= 0 {
			o.minLevel = level
		}
	}
}
