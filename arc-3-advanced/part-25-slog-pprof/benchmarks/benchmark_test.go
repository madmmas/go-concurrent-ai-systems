// Package benchmarks measures the cost of one per-article log line.
//
// Each benchmark logs the same event — article processed, with article_id,
// worker_id and latency_ms — to io.Discard, so only formatting cost is
// measured, not terminal or disk I/O.
//
//	fmt.Fprintf         — the Arc 1/2 baseline (unstructured)
//	slog Text / JSON    — structured, key=value or JSON
//	slog LogAttrs JSON  — typed attrs, avoids boxing values into `any`
//	slog disabled Debug — a Debug line when the level is Info
package benchmarks

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

const articleID, workerID, latencyMs = 42, 3, int64(317)

func BenchmarkLog_Fprintf(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fmt.Fprintf(io.Discard, "  [worker %d] article %d processed in %dms\n", workerID, articleID, latencyMs)
	}
}

func BenchmarkLog_SlogText(b *testing.B) {
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Info("article processed", "article_id", articleID, "worker_id", workerID, "latency_ms", latencyMs)
	}
}

func BenchmarkLog_SlogJSON(b *testing.B) {
	l := slog.New(slog.NewJSONHandler(io.Discard, nil))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Info("article processed", "article_id", articleID, "worker_id", workerID, "latency_ms", latencyMs)
	}
}

func BenchmarkLog_SlogJSON_WithWorker(b *testing.B) {
	// worker_id attached once via With, as the pipeline does
	l := slog.New(slog.NewJSONHandler(io.Discard, nil)).With("worker_id", workerID)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Info("article processed", "article_id", articleID, "latency_ms", latencyMs)
	}
}

func BenchmarkLog_SlogJSON_LogAttrs(b *testing.B) {
	l := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.LogAttrs(ctx, slog.LevelInfo, "article processed",
			slog.Int("article_id", articleID), slog.Int("worker_id", workerID), slog.Int64("latency_ms", latencyMs))
	}
}

func BenchmarkLog_SlogDisabledDebug(b *testing.B) {
	l := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		l.Debug("llm call", "article_id", articleID, "stage", "summarise", "latency_ms", latencyMs)
	}
}
