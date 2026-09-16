package pipeline_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
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
		if len(line) == 0 { continue }
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
		if len(line) == 0 { continue }
		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil { continue }
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

// TestPeriodicFlusher verifies ticker fires and produces log entries.
func TestPeriodicFlusher(t *testing.T) {
	var buf safeBuffer
	tickLogger := newTestLogger(&buf)

	var calls, errors int64
	calls = 42

	flusher := pipeline.NewPeriodicFlusher(50*time.Millisecond, &calls, &errors, tickLogger)
	flusher.Start()
	time.Sleep(180 * time.Millisecond)
	flusher.Stop()
	time.Sleep(20 * time.Millisecond) // let ticker goroutine exit

	data := buf.Bytes()
	lines := bytes.Split(bytes.TrimSpace(data), []byte("\n"))
	if len(lines) < 1 {
		t.Error("expected at least 1 metric flush entry")
	}
	t.Logf("flusher produced %d entries in 180ms", len(lines))

	for _, line := range lines {
		if len(line) == 0 { continue }
		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil { continue }
		if entry["msg"] == "pipeline metrics" {
			if entry["llm_calls"] != float64(42) {
				t.Errorf("llm_calls: got %v, want 42", entry["llm_calls"])
			}
			return
		}
	}
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
