package lokisdk

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestDispatcherEmitsDropSummary(t *testing.T) {
	const maxBufferBytes = 74
	d := newDispatcher(1000, time.Hour, 1<<20, maxBufferBytes, func(context.Context, []entry) error {
		return nil
	})

	base := entry{
		time:   time.Unix(0, 0),
		labels: []Label{{Key: Level, Value: string(LevelInfo)}},
		line:   "x",
	}
	d.enqueue(base)
	d.enqueue(base)

	if d.dropped != 1 {
		t.Fatalf("dropped = %d, want 1", d.dropped)
	}

	batch := d.take()
	if len(batch) != 2 {
		t.Fatalf("batch len = %d, want 2", len(batch))
	}

	var line map[string]any
	if err := json.Unmarshal([]byte(batch[1].line), &line); err != nil {
		t.Fatalf("unmarshal drop summary: %v", err)
	}
	if line["logmsg"] != "loki logs dropped" {
		t.Fatalf("logmsg = %v, want loki logs dropped", line["logmsg"])
	}
	if line["event"] != "logs_dropped" || line["reason"] != "buffer_full" {
		t.Fatalf("event/reason = %v/%v", line["event"], line["reason"])
	}
	if line["dropped_total"] != float64(1) {
		t.Fatalf("dropped_total = %v, want 1", line["dropped_total"])
	}
	dropped, ok := line["dropped"].(map[string]any)
	if !ok || dropped[string(LevelInfo)] != float64(1) {
		t.Fatalf("dropped = %#v, want info:1", line["dropped"])
	}
	if len(batch[1].labels) != 1 || batch[1].labels[0].Key != Level || batch[1].labels[0].Value != string(LevelWarn) {
		t.Fatalf("summary labels = %#v, want level=warn", batch[1].labels)
	}

	if got := d.take(); got != nil {
		t.Fatalf("second take = %#v, want nil", got)
	}
}
