// Package benchmarks compares sync primitives for the news platform.
//
// BenchmarkOnce_Hot       — factory.Get() when already initialised (ns/op)
// BenchmarkSyncMap_Read   — cache read throughput
// BenchmarkPool_GetPut    — pool recycling vs fresh allocation
// BenchmarkAtomic_Inc     — atomic.AddInt64 vs mutex-protected increment
package benchmarks

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/model"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/pipeline"
	"github.com/madmmas/go-concurrent-ai-systems/arc-3-advanced/part-22-sync-primitives/internal/simulator"
)

func BenchmarkOnce_ColdStart(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := pipeline.NewFactory(simulator.FastConfig)
		_ = f.Get()
	}
}

func BenchmarkOnce_HotPath(b *testing.B) {
	f := pipeline.NewFactory(simulator.FastConfig)
	_ = f.Get() // warm up
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Get()
	}
}

func BenchmarkSyncMap_Write(b *testing.B) {
	cache := &pipeline.DedupeCache{}
	var seq int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := atomic.AddInt64(&seq, 1)
			url := "url-" + strconv.FormatInt(n, 10)
			cache.LoadOrProcess(url, func() model.AIResult {
				return model.AIResult{ArticleID: int(n)}
			})
		}
	})
}

func BenchmarkSyncMap_Read(b *testing.B) {
	cache := &pipeline.DedupeCache{}
	cache.LoadOrProcess("url", func() model.AIResult {
		return model.AIResult{ArticleID: 1}
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cache.LoadOrProcess("url", func() model.AIResult {
				panic("process must not run on hot read")
			})
		}
	})
}

func BenchmarkPool_GetPut(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r := pipeline.GetResult()
			r.ArticleID = 1
			pipeline.PutResult(r)
		}
	})
}

func BenchmarkPool_Alloc(b *testing.B) {
	// Baseline: allocate without pool
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = &model.AIResult{ArticleID: 1}
		}
	})
}

func BenchmarkAtomic_Inc(b *testing.B) {
	var counter int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddInt64(&counter, 1)
		}
	})
}

func BenchmarkMutex_Inc(b *testing.B) {
	var counter int64
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			counter++
			mu.Unlock()
		}
	})
}

func BenchmarkPrimitivesPool_Full(b *testing.B) {
	arts := pipeline.GenerateArticles(10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		factory := pipeline.NewFactory(simulator.FastConfig)
		pool := pipeline.NewPrimitivesPool(factory, 5)
		b.StartTimer()
		pool.ProcessAll(context.Background(), arts)
	}
}
