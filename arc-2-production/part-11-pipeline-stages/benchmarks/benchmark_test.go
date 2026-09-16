// Package benchmarks measures multi-stage pipeline throughput.
//
// Central claim of Part 11: asymmetric worker counts per stage
// improve throughput because each stage is tuned to its bottleneck.
//
// BenchmarkSymmetric   — same worker count at every stage
// BenchmarkAsymmetric  — more workers at IO-bound stages, fewer at LLM stages
// BenchmarkSingleWorker — one worker per stage (near-sequential baseline)
package benchmarks

import (
	"context"
	"testing"
	"time"

	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-11-pipeline-stages/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-2-production/part-11-pipeline-stages/internal/simulator"
)

var fastCfg = simulator.Config{
	MinLatency: 5 * time.Millisecond,
	MaxLatency: 15 * time.Millisecond,
	Failure:    simulator.DefaultProfile,
}

func BenchmarkPipeline_SingleWorkerPerStage(b *testing.B) {
	stages := []pipeline.StageConfig{
		{Name: "scrape", Workers: 1}, {Name: "clean", Workers: 1},
		{Name: "embed", Workers: 1}, {Name: "summarise", Workers: 1},
	}
	p := pipeline.New(simulator.New(fastCfg), 500*time.Millisecond, stages)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkPipeline_SymmetricWorkers(b *testing.B) {
	stages := []pipeline.StageConfig{
		{Name: "scrape", Workers: 3}, {Name: "clean", Workers: 3},
		{Name: "embed", Workers: 3}, {Name: "summarise", Workers: 3},
	}
	p := pipeline.New(simulator.New(fastCfg), 500*time.Millisecond, stages)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.ProcessAll(context.Background(), arts)
	}
}

func BenchmarkPipeline_AsymmetricWorkers(b *testing.B) {
	// IO-heavy scrape gets 10 workers; LLM stages get 3 each.
	stages := []pipeline.StageConfig{
		{Name: "scrape", Workers: 10}, {Name: "clean", Workers: 5},
		{Name: "embed", Workers: 3},  {Name: "summarise", Workers: 3},
	}
	p := pipeline.New(simulator.New(fastCfg), 500*time.Millisecond, stages)
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.ProcessAll(context.Background(), arts)
	}
}
