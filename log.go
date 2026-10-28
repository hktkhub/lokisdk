package lokisdk

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// LogLevel 是日志级别枚举，作为 level 标签的值写入 Loki stream。
//
// 取值只能是下列枚举常量之一：
//
//   - LevelDebug（debug）
//   - LevelInfo（info）
//   - LevelWarn（warn）
//   - LevelError（error）
//   - LevelMetric（metric）
type LogLevel string

// 日志级别枚举值。
const (
	// LevelDebug 是调试级别。
	LevelDebug LogLevel = "debug"
	// LevelInfo 是信息级别。
	LevelInfo LogLevel = "info"
	// LevelWarn 是告警级别。
	LevelWarn LogLevel = "warn"
	// LevelError 是错误级别。
	LevelError LogLevel = "error"
	// LevelMetric 是埋点专用的特殊级别，不参与 debug < info < warn < error 的级别排序。
	//
	// 该级别始终会被写入，不受 WithMinLevel 过滤；当前也不做降采样，调用方每写一条
	// 就会发送一条。使用者应自行控制埋点频率与数据量，避免产生过高的写入与存储开销。
	LevelMetric LogLevel = "metric"
)

// levelSeverity 返回日志级别的数值权重，值越大级别越高；未知级别返回 -1。
//
// 该权重同时用于级别过滤与 WithMinLevel 的合法性校验。LevelMetric 是排序之外的
// 特殊级别，这里返回 -1，由 levelEnabled 单独放行。
func levelSeverity(level LogLevel) int {
	switch level {
	case LevelDebug:
		return 0
	case LevelInfo:
		return 1
	case LevelWarn:
		return 2
	case LevelError:
		return 3
	default:
		return -1
	}
}

// levelEnabled 判断给定级别是否达到默认发送端的最低输出级别。
//
// 未设置默认发送端或低于最低级别时返回 false，日志会被直接丢弃。
// LevelMetric 是埋点专用的特殊级别，不受 WithMinLevel 过滤，始终放行。
func levelEnabled(level LogLevel) bool {
	if defaultSender == nil {
		return false
	}
	if level == LevelMetric {
		return true
	}
	return levelSeverity(level) >= levelSeverity(defaultSender.minLevel)
}

// Field 是一条日志的结构化字段，会随日志正文一起编码进 line。
type Field struct {
	Key   string
	Value any
}

// recoverEmit 恢复日志写入路径中的 panic，保证日志链路的任何异常都不会传播到业务调用方。
//
// 恢复后 panic 会被写到 stderr，既保证业务不中断，又保留可观测性，不做静默吞没。
func recoverEmit() {
	if r := recover(); r != nil {
		recordPanic(r)
	}
}

// Debug 以 debug 级别写入一条日志，format 支持 fmt 占位符。
func Debug(ctx context.Context, format string, args ...any) {
	defer recoverEmit()
	emit(ctx, LevelDebug, fmt.Sprintf(format, args...))
}

// Info 以 info 级别写入一条日志，format 支持 fmt 占位符。
func Info(ctx context.Context, format string, args ...any) {
	defer recoverEmit()
	emit(ctx, LevelInfo, fmt.Sprintf(format, args...))
}

// Warn 以 warn 级别写入一条日志，format 支持 fmt 占位符。
func Warn(ctx context.Context, format string, args ...any) {
	defer recoverEmit()
	emit(ctx, LevelWarn, fmt.Sprintf(format, args...))
}

// Error 以 error 级别写入一条日志，format 支持 fmt 占位符。
func Error(ctx context.Context, format string, args ...any) {
	defer recoverEmit()
	emit(ctx, LevelError, fmt.Sprintf(format, args...))
}

// LogWithFields 以指定级别写入一条带结构化字段的日志。
//
// level 必须是 LevelDebug、LevelInfo、LevelWarn、LevelError、LevelMetric 之一；
// fields 会被编码成 JSON 追加到正文之后，便于在 Loki 中检索。
func LogWithFields(ctx context.Context, level LogLevel, msg string, fields ...Field) {
	defer recoverEmit()
	emit(ctx, level, msg, fields...)
}

// MetricWithFields 以 LevelMetric 级别写入一条埋点数据。
//
// 等价于 LogWithFields(ctx, LevelMetric, msg, fields...)。该级别始终写入且不做降采样，
// 调用方应自行控制埋点频率与数据量。
func MetricWithFields(ctx context.Context, msg string, fields ...Field) {
	defer recoverEmit()
	emit(ctx, LevelMetric, msg, fields...)
}

// ErrorWithCode 以 error 级别写入一条带错误码的日志。
func ErrorWithCode(ctx context.Context, code, format string, args ...any) {
	defer recoverEmit()
	fields := []Field{{Key: "code", Value: code}}
	emit(ctx, LevelError, fmt.Sprintf(format, args...), fields...)
}

// emit 补齐默认字段后交给默认发送客户端，非阻塞返回；未设置默认客户端时丢弃。
//
// 默认字段包含 logid（取自 ctx，缺失则生成 UUIDv7）、loc（文件名:行号）与 time，
// 它们与 fields 一起拼进 line。时间戳在这里统一生成一次，push 阶段不再改动。
func emit(ctx context.Context, level LogLevel, msg string, fields ...Field) {
	if defaultSender == nil {
		return
	}
	
	if !levelEnabled(level) {
		return
	}

	now := time.Now()

	defaults := make([]Field, 0)
	defaults = append(defaults,
        Field{Key: "logid", Value: LogID(ctx)},
        Field{Key: "requestid", Value: RequestID(ctx)},
        Field{Key: "loc", Value: caller()},
        Field{Key: "time", Value: now.UnixMilli()},
    )
	defaults = append(defaults, fields...)

	defaultSender.disp.enqueue(entry{
		time:   now,
		labels: []Label{{Key: Level, Value: string(level)}},
		line:   formatLine(msg, defaults),
	})
}

// logIDKey 是 logid 在 context 中的私有键类型。
type logIDKey struct{}

// WithLogID 返回携带 logid 的派生 context。
func WithLogID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, logIDKey{}, id)
}

// requestIDKey 是 requestid 在 context 中的私有键类型。
type requestIDKey struct{}

// WithRequestID 返回携带 requestid 的派生 context。
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestID 读取 ctx 中的 requestid，缺失时返回空字符串。
func RequestID(ctx context.Context) string {
	if ctx != nil {
		if id, ok := ctx.Value(requestIDKey{}).(string); ok {
			return id
		}
	}
	return ""
}

const (
	callerSkip      = 2
	callerDepth     = 16
	callerPkgPrefix = "gitee.com/tkhub/lokisdk."
)

// LogID 读取 ctx 中的 logid，缺失时现场生成一个 UUIDv7。
func LogID(ctx context.Context) string {
	if ctx != nil {
		if id, ok := ctx.Value(logIDKey{}).(string); ok && id != "" {
			return id
		}
	}
	return newLogID()
}

// newLogID 生成一个 RFC 9562 UUIDv7 字符串。
//
// 高 48 位为毫秒时间戳，使 ID 按时间有序；其余位为随机数，保证唯一性。
func newLogID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	ms := uint64(time.Now().UnixMilli())
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// caller 返回第一个非本包的调用位置，格式为 "文件名:行号"。
func caller() string {
	pcs := make([]uintptr, callerDepth)
	n := runtime.Callers(callerSkip, pcs)
	if n == 0 {
		return "unknown"
	}
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if !strings.HasPrefix(frame.Function, callerPkgPrefix) {
			return fmt.Sprintf("%s:%d", filepath.Base(frame.File), frame.Line)
		}
		if !more {
			return "unknown"
		}
	}
}

// logMsgKey 是 line JSON 中存放日志正文的保留字段名。
const logMsgKey = "logmsg"

// formatLine 将消息与结构化字段编码成一行标准 JSON 对象。
//
// 消息统一放入保留字段 logmsg，其余字段原样追加，保证整行可被 JSON 直接解析。
// 若任一字段无法序列化，回退为把消息与字段拼接成文本后放入 logmsg，避免丢信息。
func formatLine(msg string, fields []Field) string {
	m := make(map[string]any, len(fields)+1)
	m[logMsgKey] = msg
	for _, f := range fields {
		m[f.Key] = f.Value
	}
	b, err := json.Marshal(m)
	if err != nil {
		b, _ = json.Marshal(map[string]string{logMsgKey: fallbackLine(msg, fields)})
	}
	return string(b)
}

// fallbackLine 在序列化失败时把消息与字段拼成一段可读文本，避免丢信息。
func fallbackLine(msg string, fields []Field) string {
	var b strings.Builder
	b.WriteString(msg)
	for _, f := range fields {
		b.WriteByte(' ')
		b.WriteString(f.Key)
		b.WriteByte('=')
		b.WriteString(fmt.Sprint(f.Value))
	}
	return b.String()
}
