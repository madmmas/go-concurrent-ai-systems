package pipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/simulator"
)

func quiet(cfg simulator.Config) *simulator.LLMClient {
	cfg.Silent = true
	return simulator.New(cfg)
}

// TestTracer_SpanHierarchy verifies a child joins its parent's trace and
// points at the parent's span ID; a span without a parent starts a new trace.
func TestTracer_SpanHierarchy(t *testing.T) {
	recorder := &pipeline.SpanRecorder{}
	tracer := pipeline.NewTracer("test", recorder)

	ctx, root := tracer.Start(context.Background(), "root")
	_, child := tracer.Start(ctx, "child", "key", "value")
	_, other := tracer.Start(context.Background(), "other-root")

	if child.TraceID() != root.TraceID() {
		t.Errorf("child trace %s != root trace %s", child.TraceID(), root.TraceID())
	}
	if child.ParentID() != root.SpanID() {
		t.Errorf("child parent %s != root span %s", child.ParentID(), root.SpanID())
	}
	if root.ParentID() != "" {
		t.Errorf("root has parent %q", root.ParentID())
	}
	if other.TraceID() == root.TraceID() {
		t.Error("unrelated root span joined an existing trace")
	}
	if len(root.TraceID()) != 32 || len(root.SpanID()) != 16 {
		t.Errorf("IDs should be OTEL-sized hex: trace=%q span=%q", root.TraceID(), root.SpanID())
	}
}

// TestTracer_AttributesEventsStatus verifies span metadata reaches the exporter.
func TestTracer_AttributesEventsStatus(t *testing.T) {
	recorder := &pipeline.SpanRecorder{}
	tracer := pipeline.NewTracer("test", recorder)

	_, span := tracer.Start(context.Background(), "embed", "article.id", 42, "stage", "embed")
	span.AddEvent("cache.hit")
	span.SetStatus(pipeline.StatusOK, "")
	span.End()

	spans := recorder.Spans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	d := spans[0]
	if d.Attributes["article.id"] != 42 || d.Attributes["stage"] != "embed" {
		t.Errorf("attributes = %v", d.Attributes)
	}
	if len(d.Events) != 1 || d.Events[0].Name != "cache.hit" {
		t.Errorf("events = %v", d.Events)
	}
	if d.Status != pipeline.StatusOK {
		t.Errorf("status = %q", d.Status)
	}
}

// TestSpan_EndIsIdempotent verifies a second End does not export again.
func TestSpan_EndIsIdempotent(t *testing.T) {
	recorder := &pipeline.SpanRecorder{}
	_, span := pipeline.NewTracer("test", recorder).Start(context.Background(), "s")
	span.End()
	span.End()
	if n := len(recorder.Spans()); n != 1 {
		t.Errorf("span exported %d times, want 1", n)
	}
}

// TestStdoutExporter_JSONLines verifies one parseable JSON object per span.
func TestStdoutExporter_JSONLines(t *testing.T) {
	var buf bytes.Buffer
	tracer := pipeline.NewTracer("test", pipeline.NewStdoutExporter(&buf))
	ctx, root := tracer.Start(context.Background(), "root")
	_, child := tracer.Start(ctx, "child")
	child.End()
	root.End()

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("expected 2 JSON lines, got %d", len(lines))
	}
	var first pipeline.SpanData
	if err := json.Unmarshal(lines[0], &first); err != nil {
		t.Fatal(err)
	}
	if first.Name != "child" || first.ParentID != root.SpanID() {
		t.Errorf("first exported span = %+v", first)
	}
}

// TestTracedPool_TreeShape verifies every article span is a child of the
// batch span, created on a worker goroutine, and has exactly the two stage
// spans as children — i.e. ctx carried the span across goroutines.
func TestTracedPool_TreeShape(t *testing.T) {
	pool := pipeline.New(quiet(simulator.FastConfig), 3, 2*time.Second)
	const n = 5
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(n))

	spans := pool.Recorder().Spans()
	if want := 1 + n*3; len(spans) != want {
		t.Fatalf("expected %d spans for %d articles, got %d", want, n, len(spans))
	}

	byID := map[string]pipeline.SpanData{}
	var batch pipeline.SpanData
	for _, s := range spans {
		byID[s.SpanID] = s
		if s.Name == "process-batch" {
			batch = s
		}
	}
	children := map[string][]string{}
	for _, s := range spans {
		if s.TraceID != batch.TraceID {
			t.Errorf("span %s is in trace %s, want batch trace %s", s.Name, s.TraceID, batch.TraceID)
		}
		if s.ParentID != "" {
			children[s.ParentID] = append(children[s.ParentID], s.Name)
		}
	}
	if len(children[batch.SpanID]) != n {
		t.Errorf("batch has %d children, want %d", len(children[batch.SpanID]), n)
	}
	for _, s := range spans {
		if s.Name != "process-article" {
			continue
		}
		if s.ParentID != batch.SpanID {
			t.Errorf("article span parent = %s, want batch", s.ParentID)
		}
		kids := children[s.SpanID]
		if len(kids) != 2 || kids[0] == kids[1] {
			t.Errorf("article %v children = %v, want summarise + embed", s.Attributes["article.id"], kids)
		}
	}
}

// TestTracedPool_ErrorStatusPropagates verifies a failed stage marks both
// the stage span and its article span as Error.
func TestTracedPool_ErrorStatusPropagates(t *testing.T) {
	cfg := simulator.FastConfig
	cfg.Failure = simulator.FailureProfile{ServerErrRate: 1.0}
	pool := pipeline.New(quiet(cfg), 2, 2*time.Second)
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(2))

	errs := map[string]int{}
	for _, s := range pool.Recorder().Spans() {
		if s.Status == pipeline.StatusError {
			errs[s.Name]++
		}
		if s.Name == "embed" {
			t.Error("embed span created after summarise failed")
		}
	}
	if errs["summarise"] != 2 || errs["process-article"] != 2 {
		t.Errorf("error spans = %v, want 2 summarise + 2 process-article", errs)
	}
}

// TestTracedPool_SpanDurationsNest verifies children fall inside parents.
func TestTracedPool_SpanDurationsNest(t *testing.T) {
	pool := pipeline.New(quiet(simulator.FastConfig), 3, 2*time.Second)
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(4))

	spans := pool.Recorder().Spans()
	byID := map[string]pipeline.SpanData{}
	for _, s := range spans {
		byID[s.SpanID] = s
	}
	for _, s := range spans {
		if s.Duration() <= 0 {
			t.Errorf("span %q has non-positive duration %v", s.Name, s.Duration())
		}
		if p, ok := byID[s.ParentID]; ok {
			if s.StartTime.Before(p.StartTime) || s.EndTime.After(p.EndTime) {
				t.Errorf("span %q [%v..%v] escapes parent %q [%v..%v]",
					s.Name, s.StartTime, s.EndTime, p.Name, p.StartTime, p.EndTime)
			}
		}
	}
}

// TestTracedPool_NoRace verifies no data races.
func TestTracedPool_NoRace(t *testing.T) {
	var buf bytes.Buffer
	pool := pipeline.New(quiet(simulator.FastConfig), 8, 2*time.Second, pipeline.NewStdoutExporter(&buf))
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(20))
	if len(results) != 20 {
		t.Errorf("expected 20, got %d", len(results))
	}
}
