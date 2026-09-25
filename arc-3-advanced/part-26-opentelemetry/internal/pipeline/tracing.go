// Package pipeline implements OpenTelemetry-style tracing for Part 26.
//
// OpenTelemetry (OTEL) is the industry standard for distributed tracing.
// One instrumentation, many backends: Jaeger, Grafana Tempo, Honeycomb, Datadog.
//
// This package implements a minimal tracer with the stdlib only, so the code
// compiles and tests pass without external packages. It follows the SHAPE of
// the OTEL API — Tracer.Start(ctx, name) returns (ctx, span); spans carry
// attributes, events and a status; parent/child links travel in ctx; an
// exporter receives finished spans — but it is not type-compatible. Moving to
// the real SDK means mechanical call-site changes:
//
//	tracer.Start(ctx, "embed", "article.id", id)
//	  → tracer.Start(ctx, "embed", trace.WithAttributes(attribute.Int("article.id", id)))
//	span.SetStatus(StatusError, msg)
//	  → span.SetStatus(codes.Error, msg)
//	NewTracer("news-pipeline", exporter)
//	  → otel.Tracer("news-pipeline") with a TracerProvider:
//	    exp, _ := stdouttrace.New(stdouttrace.WithPrettyPrint())      // stdout
//	    exp, _ := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure()) // Jaeger/Tempo
//	    tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
//	    otel.SetTracerProvider(tp); defer tp.Shutdown(ctx)
//
// The pipeline structure — which spans exist, who their parents are, how ctx
// carries them across goroutines — does not change.
package pipeline

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/simulator"
)

// ─── Minimal OTEL-style tracer (stdlib only) ─────────────────────────────────

// Status codes, mirroring go.opentelemetry.io/otel/codes.
const (
	StatusUnset = "Unset"
	StatusOK    = "Ok"
	StatusError = "Error"
)

// Span represents a single unit of work in a trace.
type Span struct {
	mu         sync.Mutex
	name       string
	traceID    string // 16 bytes, hex — same size as OTEL trace IDs
	spanID     string // 8 bytes, hex — same size as OTEL span IDs
	parentID   string
	start      time.Time
	end        time.Time
	ended      bool
	attrs      map[string]interface{}
	events     []spanEvent
	statusCode string
	statusMsg  string
	exporter   Exporter
}

type spanEvent struct {
	Name string    `json:"Name"`
	Time time.Time `json:"Time"`
}

// SetAttributes records key-value pairs on the span.
func (s *Span) SetAttributes(kvs ...interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i+1 < len(kvs); i += 2 {
		key, _ := kvs[i].(string)
		s.attrs[key] = kvs[i+1]
	}
}

// AddEvent records a named event on the span timeline.
func (s *Span) AddEvent(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, spanEvent{Name: name, Time: time.Now()})
}

// SetStatus records the span outcome (StatusOK or StatusError).
func (s *Span) SetStatus(code, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCode = code
	s.statusMsg = msg
}

// End closes the span and hands it to the exporter.
// Like OTEL, only the first End has any effect.
func (s *Span) End() {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.end = time.Now()
	s.mu.Unlock()
	if s.exporter != nil {
		s.exporter.ExportSpan(s.Snapshot())
	}
}

// SpanData is an immutable copy of a finished span — what exporters receive.
// Field names follow the OTEL stdouttrace JSON output.
type SpanData struct {
	Name       string                 `json:"Name"`
	TraceID    string                 `json:"TraceID"`
	SpanID     string                 `json:"SpanID"`
	ParentID   string                 `json:"ParentSpanID,omitempty"`
	StartTime  time.Time              `json:"StartTime"`
	EndTime    time.Time              `json:"EndTime"`
	Attributes map[string]interface{} `json:"Attributes,omitempty"`
	Events     []spanEvent            `json:"Events,omitempty"`
	Status     string                 `json:"Status"`
	StatusMsg  string                 `json:"StatusDescription,omitempty"`
}

// Duration is EndTime - StartTime.
func (d SpanData) Duration() time.Duration { return d.EndTime.Sub(d.StartTime) }

// Snapshot returns a copy of the span's current state.
func (s *Span) Snapshot() SpanData {
	s.mu.Lock()
	defer s.mu.Unlock()
	attrs := make(map[string]interface{}, len(s.attrs))
	for k, v := range s.attrs {
		attrs[k] = v
	}
	status := s.statusCode
	if status == "" {
		status = StatusUnset
	}
	return SpanData{
		Name: s.name, TraceID: s.traceID, SpanID: s.spanID, ParentID: s.parentID,
		StartTime: s.start, EndTime: s.end,
		Attributes: attrs, Events: append([]spanEvent(nil), s.events...),
		Status: status, StatusMsg: s.statusMsg,
	}
}

// Name, TraceID, SpanID, ParentID and Duration expose span identity.
func (s *Span) Name() string     { s.mu.Lock(); defer s.mu.Unlock(); return s.name }
func (s *Span) TraceID() string  { return s.traceID } // immutable after Start
func (s *Span) SpanID() string   { return s.spanID }
func (s *Span) ParentID() string { return s.parentID }
func (s *Span) Duration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.end.Sub(s.start)
}

// ─── Exporters ────────────────────────────────────────────────────────────────

// Exporter receives every finished span. Mirrors the role of an OTEL
// SpanExporter (the real interface takes batches; one span keeps this small).
type Exporter interface {
	ExportSpan(SpanData)
}

// SpanRecorder keeps finished spans in memory — for tests and for the
// end-of-run tree view. Mirrors sdk/trace/tracetest.SpanRecorder.
type SpanRecorder struct {
	mu    sync.Mutex
	spans []SpanData
}

// ExportSpan stores a finished span.
func (r *SpanRecorder) ExportSpan(d SpanData) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, d)
}

// Spans returns all recorded spans, in the order they ended.
func (r *SpanRecorder) Spans() []SpanData {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]SpanData(nil), r.spans...)
}

// Clear removes all recorded spans.
func (r *SpanRecorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = nil
}

// StdoutExporter writes one JSON object per finished span, like the OTEL
// stdouttrace exporter (without pretty-printing). Safe for concurrent use:
// spans end on many worker goroutines at once.
type StdoutExporter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

// NewStdoutExporter writes span JSON to w.
func NewStdoutExporter(w io.Writer) *StdoutExporter {
	return &StdoutExporter{enc: json.NewEncoder(w)}
}

// ExportSpan writes d as a single JSON line.
func (e *StdoutExporter) ExportSpan(d SpanData) {
	e.mu.Lock()
	defer e.mu.Unlock()
	_ = e.enc.Encode(d)
}

// MultiExporter fans a span out to several exporters.
type MultiExporter []Exporter

// ExportSpan forwards d to every exporter.
func (m MultiExporter) ExportSpan(d SpanData) {
	for _, e := range m {
		e.ExportSpan(d)
	}
}

// ─── Tracer ───────────────────────────────────────────────────────────────────

// Tracer creates spans for a named instrumentation library.
type Tracer struct {
	name     string
	exporter Exporter
}

// NewTracer returns a Tracer that sends finished spans to exporter.
func NewTracer(name string, exporter Exporter) *Tracer {
	return &Tracer{name: name, exporter: exporter}
}

func randomHex(nBytes int) string {
	b := make([]byte, nBytes)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Start creates a span. If ctx carries a span, the new span is its child
// and joins its trace; otherwise it is the root of a new trace.
// The returned ctx carries the new span — pass it to everything the span
// covers, including goroutines, so their spans nest under it.
func (t *Tracer) Start(ctx context.Context, spanName string, kvs ...interface{}) (context.Context, *Span) {
	parent := SpanFromContext(ctx)
	span := &Span{
		name:     spanName,
		traceID:  randomHex(16),
		spanID:   randomHex(8),
		start:    time.Now(),
		attrs:    make(map[string]interface{}),
		exporter: t.exporter,
	}
	if parent != nil {
		span.traceID = parent.traceID
		span.parentID = parent.spanID
	}
	span.SetAttributes(kvs...)
	return context.WithValue(ctx, contextKeySpan{}, span), span
}

type contextKeySpan struct{}

// SpanFromContext returns the span carried by ctx, or nil.
// Mirrors trace.SpanFromContext.
func SpanFromContext(ctx context.Context) *Span {
	s, _ := ctx.Value(contextKeySpan{}).(*Span)
	return s
}

// ─── Tree view ────────────────────────────────────────────────────────────────

// RenderTree draws each trace as an indented waterfall: every span shows
// when it started and ended relative to its trace's root, in milliseconds.
func RenderTree(w io.Writer, spans []SpanData, attrKeys ...string) {
	children := map[string][]SpanData{}
	var roots []SpanData
	for _, s := range spans {
		if s.ParentID == "" {
			roots = append(roots, s)
		} else {
			children[s.ParentID] = append(children[s.ParentID], s)
		}
	}
	for k := range children {
		c := children[k]
		sort.Slice(c, func(i, j int) bool { return c[i].StartTime.Before(c[j].StartTime) })
	}
	var walk func(s SpanData, t0 time.Time, prefix string, last bool, depth int)
	walk = func(s SpanData, t0 time.Time, prefix string, last bool, depth int) {
		branch, next := "", ""
		if depth > 0 {
			branch, next = "├─ ", "│  "
			if last {
				branch, next = "└─ ", "   "
			}
		}
		var tags []string
		for _, k := range attrKeys {
			if v, ok := s.Attributes[k]; ok {
				tags = append(tags, fmt.Sprintf("%s=%v", k, v))
			}
		}
		if s.Status == StatusError {
			tags = append(tags, "status=Error")
		}
		label := prefix + branch + s.Name
		if len(tags) > 0 {
			label += " [" + strings.Join(tags, " ") + "]"
		}
		fmt.Fprintf(w, "%-58s %5dms → %5dms  (%dms)\n", label,
			s.StartTime.Sub(t0).Milliseconds(), s.EndTime.Sub(t0).Milliseconds(),
			s.Duration().Milliseconds())
		kids := children[s.SpanID]
		for i, c := range kids {
			walk(c, t0, prefix+next, i == len(kids)-1, depth+1)
		}
	}
	for _, r := range roots {
		fmt.Fprintf(w, "trace %s\n", r.TraceID)
		walk(r, r.StartTime, "", true, 0)
	}
}

// ─── Instrumented pipeline ────────────────────────────────────────────────────

// TracedPool processes articles with OTEL-style tracing.
// One batch is one trace:
//
//	process-batch                      (root, main goroutine)
//	  └── process-article              (one per article, on a worker goroutine)
//	        ├── summarise
//	        └── embed
type TracedPool struct {
	llm      *simulator.LLMClient
	Workers  int
	Timeout  time.Duration
	tracer   *Tracer
	recorder *SpanRecorder
}

// New returns a TracedPool. Finished spans always go to an in-memory
// recorder; pass extra exporters (e.g. NewStdoutExporter) to also stream them.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration, extra ...Exporter) *TracedPool {
	recorder := &SpanRecorder{}
	exporter := append(MultiExporter{recorder}, extra...)
	return &TracedPool{
		llm: llm, Workers: workers, Timeout: timeout,
		tracer: NewTracer("news-pipeline", exporter), recorder: recorder,
	}
}

// ProcessAll processes articles and records one trace for the batch.
func (p *TracedPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	batchCtx, batchSpan := p.tracer.Start(ctx, "process-batch",
		"batch.size", len(articles),
		"workers", p.Workers,
	)
	defer batchSpan.End()

	jobs := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for article := range jobs {
				// batchCtx crosses into this goroutine: the article span
				// created from it becomes a child of process-batch.
				articleCtx, cancel := context.WithTimeout(batchCtx, p.Timeout)
				results <- p.processArticle(articleCtx, article, workerID)
				cancel()
			}
		}(w + 1)
	}

	for _, a := range articles {
		jobs <- a
	}
	close(jobs)
	go func() { wg.Wait(); close(results) }()

	var out []model.AIResult
	for r := range results {
		out = append(out, r)
	}
	return out, time.Since(start)
}

func (p *TracedPool) processArticle(ctx context.Context, a model.Article, workerID int) model.AIResult {
	ctx, articleSpan := p.tracer.Start(ctx, "process-article",
		"article.id", a.ID,
		"article.url", a.URL,
		"worker.id", workerID,
	)
	defer articleSpan.End()

	r := model.AIResult{ArticleID: a.ID}

	if err := p.stage(ctx, "summarise", a.ID); err != nil {
		articleSpan.SetStatus(StatusError, err.Error())
		r.Err = err
		return r
	}
	r.Summary = "AI summary"

	if err := p.stage(ctx, "embed", a.ID); err != nil {
		articleSpan.SetStatus(StatusError, err.Error())
		r.Err = err
		return r
	}
	r.Embedding = []float32{0.1, 0.2, 0.3}

	articleSpan.SetStatus(StatusOK, "")
	return r
}

// stage wraps one LLM call in a child span. The span's ctx — not the
// parent's — goes into the call, so anything the call traces nests here.
func (p *TracedPool) stage(ctx context.Context, name string, articleID int) error {
	ctx, span := p.tracer.Start(ctx, name, "article.id", articleID)
	defer span.End()
	if err := p.llm.Call(ctx, name, articleID); err != nil {
		span.SetStatus(StatusError, err.Error())
		return err
	}
	span.AddEvent(name + ".complete")
	span.SetStatus(StatusOK, "")
	return nil
}

// Recorder returns the in-memory span recorder.
func (p *TracedPool) Recorder() *SpanRecorder { return p.recorder }

// GenerateArticles produces n dummy articles.
func GenerateArticles(n int) []model.Article {
	articles := make([]model.Article, n)
	for i := range articles {
		articles[i] = model.Article{
			ID:  i + 1,
			URL: fmt.Sprintf("https://news.example.com/%d", i+1),
		}
	}
	return articles
}
