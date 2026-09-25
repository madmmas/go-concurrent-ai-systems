package pipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-25-slog-pprof/internal/simulator"
)

// safeBuffer wraps bytes.Buffer with a mutex for concurrent writes.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := make([]byte, s.buf.Len())
	copy(b, s.buf.Bytes())
	return b
}

func newTestLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestSlogOutput verifies slog writes valid JSON with required keys.
func TestSlogOutput(t *testing.T) {
	var buf safeBuffer
	logger := newTestLogger(&buf)
	pool := pipeline.New(simulator.New(simulator.FastConfig), 2, 2*time.Second, logger)
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(3))

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) == 0 {
		t.Fatal("no log output produced")
	}
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Errorf("not valid JSON: %s", line)
		}
	}
	t.Logf("produced %d log lines for 3 articles", len(lines))
}

// TestSlogPipelineStarted verifies "pipeline started" entry has correct fields.
func TestSlogPipelineStarted(t *testing.T) {
	var buf safeBuffer
	logger := newTestLogger(&buf)
	pool := pipeline.New(simulator.New(simulator.FastConfig), 3, 2*time.Second, logger)
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(5))

	found := false
	for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if entry["msg"] == "pipeline started" {
			found = true
			if entry["articles"] != float64(5) {
				t.Errorf("articles field: got %v, want 5", entry["articles"])
			}
		}
	}
	if !found {
		t.Error("'pipeline started' log entry not found")
	}
}

// TestPeriodicFlusher verifies the ticker fires and numbers stay numbers.
func TestPeriodicFlusher(t *testing.T) {
	var buf safeBuffer
	var processed, failed int64 = 42, 3

	flusher := pipeline.NewPeriodicFlusher(50*time.Millisecond, &processed, &failed, newTestLogger(&buf))
	flusher.Start()
	time.Sleep(180 * time.Millisecond)
	flusher.Stop()

	entries := parseLines(t, buf.Bytes())
	var ticks int
	for _, e := range entries {
		if e["msg"] != "pipeline metrics" {
			continue
		}
		ticks++
		if e["articles_processed"] != float64(42) {
			t.Errorf("articles_processed: got %v, want 42", e["articles_processed"])
		}
		if e["error_rate_pct"] != 7.1 { // 3/42 = 7.14% → 7.1, as a number
			t.Errorf("error_rate_pct: got %v (%T), want 7.1 as a number", e["error_rate_pct"], e["error_rate_pct"])
		}
	}
	if ticks < 3 { // ~3 ticks + 1 final
		t.Errorf("expected at least 3 metric entries in 180ms, got %d", ticks)
	}
}

// TestPeriodicFlusher_FinalFlushOnStop verifies Stop emits the last numbers
// even when no tick has fired yet, and waits for it before returning.
func TestPeriodicFlusher_FinalFlushOnStop(t *testing.T) {
	var buf safeBuffer
	var processed, failed int64 = 7, 0

	flusher := pipeline.NewPeriodicFlusher(time.Hour, &processed, &failed, newTestLogger(&buf))
	flusher.Start()
	flusher.Stop() // no sleep: Stop must block until the final flush is written

	entries := parseLines(t, buf.Bytes())
	if len(entries) != 1 || entries[0]["final"] != true || entries[0]["articles_processed"] != float64(7) {
		t.Fatalf("want exactly one final flush with articles_processed=7, got %v", entries)
	}
	flusher.Stop() // second Stop must not panic
}

// TestSlog_LLMCallsCarryStage verifies simulator logs go through slog with
// article_id and stage — no stray fmt.Printf lines in JSON mode.
func TestSlog_LLMCallsCarryStage(t *testing.T) {
	var buf safeBuffer
	pool := pipeline.New(simulator.New(simulator.FastConfig), 2, 2*time.Second, newTestLogger(&buf))
	pool.ProcessAll(context.Background(), pipeline.GenerateArticles(3))

	llmLines := 0
	for _, e := range parseLines(t, buf.Bytes()) {
		if e["msg"] == "llm call" {
			llmLines++
			if e["stage"] != "summarise" || e["article_id"] == nil || e["latency_ms"] == nil {
				t.Errorf("llm call line missing fields: %v", e)
			}
		}
	}
	if llmLines != 3 {
		t.Errorf("expected 3 'llm call' lines, got %d", llmLines)
	}
}

// TestPprofServer verifies the goroutine profile is served on a dedicated mux.
func TestPprofServer(t *testing.T) {
	srv, err := pipeline.StartPprofServer("127.0.0.1:0", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if srv.Handler == http.DefaultServeMux || srv.Handler == nil {
		t.Error("pprof must not be served from http.DefaultServeMux")
	}
	resp, err := http.Get("http://" + srv.Addr + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !bytes.HasPrefix(body, []byte("goroutine profile:")) {
		t.Errorf("status %d, body starts %q", resp.StatusCode, body[:min(40, len(body))])
	}
}

// TestPprofServer_BadAddrFailsFast verifies a bind error is returned, not logged later.
func TestPprofServer_BadAddrFailsFast(t *testing.T) {
	if _, err := pipeline.StartPprofServer("256.0.0.1:1", slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Error("expected error for invalid address")
	}
}

func parseLines(t *testing.T, data []byte) []map[string]interface{} {
	t.Helper()
	var out []map[string]interface{}
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var e map[string]interface{}
		if err := json.Unmarshal(line, &e); err != nil {
			t.Errorf("not valid JSON: %s", line)
			continue
		}
		out = append(out, e)
	}
	return out
}

// TestObservablePool_AllResultsDelivered verifies end-to-end correctness.
func TestObservablePool_AllResultsDelivered(t *testing.T) {
	pool := pipeline.New(
		simulator.New(simulator.FastConfig), 4, 2*time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(10))
	if len(results) != 10 {
		t.Errorf("expected 10, got %d", len(results))
	}
}

// TestObservablePool_NoRace verifies no data races.
func TestObservablePool_NoRace(t *testing.T) {
	pool := pipeline.New(
		simulator.New(simulator.FastConfig), 8, 2*time.Second,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	results, _ := pool.ProcessAll(context.Background(), pipeline.GenerateArticles(30))
	if len(results) != 30 {
		t.Errorf("expected 30, got %d", len(results))
	}
}
