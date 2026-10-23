package lokisdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// entryOverhead 是单条 entry 在缓冲中的固定开销估算值（时间戳、级别、切片槽位等）。
const entryOverhead = 64

// ErrDispatchPanic 表示日志发送回调在发送过程中发生 panic，已被恢复为错误返回。
//
// 把 panic 收敛为错误是刻意设计：发送既可能发生在后台 goroutine，也可能发生在
// 业务调用链上，任何未恢复的 panic 都会终止进程或打断业务，与“日志不得影响业务”相背。
var ErrDispatchPanic = errors.New("loki: dispatch panic")

// recordPanic 把被恢复的 panic 写到 stderr，作为 Loki 不可用时的兜底可观测通道。
func recordPanic(r any) {
	fmt.Fprintf(os.Stderr, "lokisdk: recovered panic: %v\n", r)
}

// dispatcher 是日志发送的调度中心。
//
// 它不构造 HTTP 请求，只在内存中缓存 entry，并按以下任一条件触发一次 push：
// 条数达到 batchSize、体积达到 maxBatchBytes、或到达 flushInterval。
// 缓冲总量超过 maxBufferBytes 时，新的 entry 会被丢弃并计入 dropped。
type dispatcher struct {
	batchSize      int
	flushInterval  time.Duration
	maxBatchBytes  int64
	maxBufferBytes int64
	push           func(context.Context, []entry) error

	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	buf      []entry
	bufBytes int64
	dropped  uint64
	failed   uint64
	closed   bool

	notify   chan struct{}
	done     chan struct{}
	finished chan struct{}

	startOnce sync.Once
	closeOnce sync.Once
}

// newDispatcher 依据配置与 push 回调创建一个调度中心，但不会启动后台循环。
func newDispatcher(
	batchSize int,
	flushInterval time.Duration,
	maxBatchBytes, maxBufferBytes int64,
	push func(context.Context, []entry) error,
) *dispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	return &dispatcher{
		batchSize:      batchSize,
		flushInterval:  flushInterval,
		maxBatchBytes:  maxBatchBytes,
		maxBufferBytes: maxBufferBytes,
		push:           push,
		ctx:            ctx,
		cancel:         cancel,
		notify:         make(chan struct{}, 1),
		done:           make(chan struct{}),
		finished:       make(chan struct{}),
	}
}

// start 启动后台发送循环；重复调用只生效一次。
func (d *dispatcher) start() {
	d.startOnce.Do(func() {
		go d.run()
	})
}

// run 是后台循环：在被唤醒、到点或收到停止信号时发送缓冲。
func (d *dispatcher) run() {
	defer close(d.finished)

	ticker := time.NewTicker(d.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.done:
			_ = d.send(d.ctx)
			return
		case <-ticker.C:
			_ = d.send(d.ctx)
		case <-d.notify:
			_ = d.send(d.ctx)
		}
	}
}

// enqueue 非阻塞地写入一条日志；缓冲已满或已关闭时丢弃并计入 dropped。
func (d *dispatcher) enqueue(e entry) {
	size := entrySize(e)

	d.mu.Lock()
	if d.closed || d.bufBytes+size > d.maxBufferBytes {
		d.dropped++
		d.mu.Unlock()
		return
	}
	d.buf = append(d.buf, e)
	d.bufBytes += size
	ready := len(d.buf) >= d.batchSize || d.bufBytes >= d.maxBatchBytes
	d.mu.Unlock()

	if ready {
		d.signal()
	}
}

// send 取出当前缓冲并推送一次；失败时按条数计入 failed 并返回错误。
//
// push 过程中发生的 panic 会被恢复为 ErrDispatchPanic 并计入 failed，
// 以保证后台发送循环与业务调用方都不会被日志链路中的 panic 打断。
func (d *dispatcher) send(ctx context.Context) (err error) {
	var batch []entry
	defer func() {
		if r := recover(); r != nil {
			d.mu.Lock()
			d.failed += uint64(len(batch))
			d.mu.Unlock()
			recordPanic(r)
			err = fmt.Errorf("loki: send panic recovered: %v: %w", r, ErrDispatchPanic)
		}
	}()

	batch = d.take()
	if len(batch) == 0 {
		return nil
	}
	if pushErr := d.push(ctx, batch); pushErr != nil {
		d.mu.Lock()
		d.failed += uint64(len(batch))
		d.mu.Unlock()
		return pushErr
	}
	return nil
}

// close 停止后台循环并做最后一次发送；ctx 用于限定等待退出时间。
func (d *dispatcher) close(ctx context.Context) error {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.mu.Unlock()
		close(d.done)

		d.startOnce.Do(func() {
			_ = d.send(ctx)
			close(d.finished)
		})
	})

	select {
	case <-d.finished:
		d.cancel()
		return nil
	case <-ctx.Done():
		d.cancel()
		return fmt.Errorf("loki: close dispatcher: %w", ctx.Err())
	}
}

// take 取出当前缓冲并重置体积计数。
func (d *dispatcher) take() []entry {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.buf) == 0 {
		return nil
	}
	batch := d.buf
	d.buf = nil
	d.bufBytes = 0
	return batch
}

// signal 非阻塞地通知后台循环立即发送。
func (d *dispatcher) signal() {
	select {
	case d.notify <- struct{}{}:
	default:
	}
}

// entrySize 估算一条 entry 的内存占用，用于内存上限控制。
func entrySize(e entry) int64 {
	size := int64(len(e.line)) + entryOverhead
	for _, l := range e.labels {
		size += int64(len(l.Key)) + int64(len(l.Value))
	}
	return size
}
