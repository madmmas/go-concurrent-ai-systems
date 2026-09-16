// Package pipeline implements OpenTelemetry tracing for Part 26.
//
// OpenTelemetry (OTEL) is the industry standard for distributed tracing.
// One instrumentation, many backends: Jaeger, Grafana Tempo, Honeycomb, Datadog.
//
// This package implements a minimal tracer using stdlib interfaces so the
// code compiles and tests pass without external packages. The design exactly
// mirrors the real OTEL SDK API — swapping in the real SDK requires only
// changing the constructor calls, not the pipeline code.
//
// Real OTEL integration (once go.opentelemetry.io/otel is available):
//
//   import (
//     "go.opentelemetry.io/otel"
//     "go.opentelemetry.io/otel/attribute"
//     "go.opentelemetry.io/otel/trace"
//   )
//
// Stdout exporter (no backend needed — traces print to terminal):
//
//   exporter, _ := stdouttrace.New(stdouttrace.WithPrettyPrint())
//   tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter))
//   otel.SetTracerProvider(tp)
//
// Upgrade path to Jaeger (one docker run line):
//
//   docker run --rm -p 16686:16686 -p 4317:4317 jaegertracing/all-in-one
//   exporter, _ := otlptracegrpc.New(ctx,
//     otlptracegrpc.WithInsecure(),
//     otlptracegrpc.WithEndpoint("localhost:4317"),
//   )
package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-26-opentelemetry/internal/simulator"
)

// ─── Minimal OTEL-compatible tracer (stdlib only) ────────────────────────────

// Span represents a single unit of work in a trace.
// Mirrors otel/trace.Span interface.
type Span struct {
	mu         sync.Mutex
	name       string
	traceID    string
	spanID     string
	parentID   string
	start      time.Time
	end        time.Time
	attrs      map[string]interface{}
	events     []spanEvent
	statusCode string
	statusMsg  string
	recorder   *SpanRecorder
}

type spanEvent struct {
	name string
	time time.Time
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
	s.events = append(s.events, spanEvent{name: name, time: time.Now()})
}

// SetStatus records the span outcome.
func (s *Span) SetStatus(code, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusCode = code
	s.statusMsg = msg
}

// End closes the span and records it.
func (s *Span) End() {
	s.mu.Lock()
	s.end = time.Now()
	s.mu.Unlock()
	if s.recorder != nil {
		s.recorder.record(s)
	}
}

// Duration returns the span duration.
func (s *Span) Duration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.end.Sub(s.start)
}

// SpanRecorder collects finished spans for inspection in tests.
type SpanRecorder struct {
	mu    sync.Mutex
	spans []*Span
}

// record stores a finished span.
func (r *SpanRecorder) record(s *Span) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, s)
}

// Spans returns all recorded spans.
func (r *SpanRecorder) Spans() []*Span {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Span, len(r.spans))
	copy(out, r.spans)
	return out
}

// Clear removes all recorded spans.
func (r *SpanRecorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = nil
}

// Tracer creates spans for a named instrumentation library.
// Mirrors otel/trace.Tracer.
type Tracer struct {
	name     string
	recorder *SpanRecorder
	mu       sync.Mutex
	counter  int
}

// NewTracer returns a Tracer that records spans to recorder.
func NewTracer(name string, recorder *SpanRecorder) *Tracer {
	return &Tracer{name: name, recorder: recorder}
}

// Start creates a child span. The returned context carries the span.
func (t *Tracer) Start(ctx context.Context, spanName string, kvs ...interface{}) (context.Context, *Span) {
	t.mu.Lock()
	t.counter++
	id := fmt.Sprintf("%s-%04d", t.name, t.counter)
	t.mu.Unlock()

	parentSpan, _ := ctx.Value(contextKeySpan{}).(*Span)
	parentID := ""
	traceID := id
	if parentSpan != nil {
		parentID = parentSpan.spanID
		traceID  = parentSpan.traceID
	}

	span := &Span{
		name:     spanName,
		traceID:  traceID,
		spanID:   id,
		parentID: parentID,
		start:    time.Now(),
		attrs:    make(map[string]interface{}),
		recorder: t.recorder,
	}
	span.SetAttributes(kvs...)

	ctx = context.WithValue(ctx, contextKeySpan{}, span)
	return ctx, span
}

type contextKeySpan struct{}

// ─── Instrumented pipeline ────────────────────────────────────────────────────

// TracedPool processes articles with OTEL-style distributed tracing.
// Each article produces a root span with child spans per stage:
//
//	process-article
//	  └── summarise
//	  └── embed
type TracedPool struct {
	llm      *simulator.LLMClient
	Workers  int
	Timeout  time.Duration
	tracer   *Tracer
	recorder *SpanRecorder
}

// New returns a TracedPool.
func New(llm *simulator.LLMClient, workers int, timeout time.Duration) *TracedPool {
	recorder := &SpanRecorder{}
	tracer   := NewTracer("news-pipeline", recorder)
	return &TracedPool{
		llm: llm, Workers: workers, Timeout: timeout,
		tracer: tracer, recorder: recorder,
	}
}

// ProcessAll processes articles and records a trace per article.
func (p *TracedPool) ProcessAll(ctx context.Context, articles []model.Article) ([]model.AIResult, time.Duration) {
	start := time.Now()

	// Root span for the entire batch
	batchCtx, batchSpan := p.tracer.Start(ctx, "process-batch",
		"batch.size", len(articles),
		"workers",    p.Workers,
	)
	defer batchSpan.End()

	jobs    := make(chan model.Article, len(articles))
	results := make(chan model.AIResult, len(articles))

	var wg sync.WaitGroup
	for w := 0; w < p.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for article := range jobs {
				articleCtx, cancel := context.WithTimeout(batchCtx, p.Timeout)
				results <- p.processArticle(articleCtx, article, workerID)
				cancel()
			}
		}(w + 1)
	}

	for _, a := range articles { jobs <- a }
	close(jobs)
	go func() { wg.Wait(); close(results) }()

	var out []model.AIResult
	for r := range results { out = append(out, r) }
	return out, time.Since(start)
}

func (p *TracedPool) processArticle(ctx context.Context, a model.Article, workerID int) model.AIResult {
	// Article span — child of batch span
	articleCtx, articleSpan := p.tracer.Start(ctx, "process-article",
		"article.id",  a.ID,
		"article.url", a.URL,
		"worker.id",   workerID,
	)
	defer articleSpan.End()

	r := model.AIResult{ArticleID: a.ID}

	// Summarise span — child of article span
	_, sumSpan := p.tracer.Start(articleCtx, "summarise",
		"article.id", a.ID,
	)
	if err := p.llm.Call(articleCtx, "Summarise", a.ID); err != nil {
		sumSpan.SetStatus("ERROR", err.Error())
		sumSpan.End()
		articleSpan.SetStatus("ERROR", err.Error())
		r.Err = err
		return r
	}
	sumSpan.AddEvent("summarise.complete")
	sumSpan.End()
	r.Summary = "AI summary"

	// Embed span — child of article span
	_, embedSpan := p.tracer.Start(articleCtx, "embed",
		"article.id", a.ID,
	)
	if err := p.llm.Call(articleCtx, "Embed", a.ID); err != nil {
		embedSpan.SetStatus("ERROR", err.Error())
		embedSpan.End()
		articleSpan.SetStatus("ERROR", err.Error())
		r.Err = err
		return r
	}
	embedSpan.AddEvent("embed.complete")
	embedSpan.End()
	r.Embedding = []float32{0.1, 0.2, 0.3}

	articleSpan.SetStatus("OK", "")
	articleSpan.SetAttributes("article.tokens", 150)
	return r
}

// Recorder returns the span recorder for test inspection.
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

// Name returns the span's name.
func (s *Span) Name() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.name
}
