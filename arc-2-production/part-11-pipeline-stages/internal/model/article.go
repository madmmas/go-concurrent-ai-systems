// Package model defines the core data types for the news intelligence platform.
// These types evolve across arcs as the platform gains capabilities.
package model

import "time"

// Article represents a news article ingested by the platform.
type Article struct {
	ID      int
	Title   string
	Content string
	URL     string    // added Arc 2: scraping pipeline needs URLs
	Source  string    // added Arc 2: track provenance
	FetchedAt time.Time
}

// AIResult holds the output of all AI tasks run against a single article.
// Err is non-nil if any task failed (timeout, rate limit, hard error).
type AIResult struct {
	ArticleID int
	Summary   string
	Sentiment string
	Keywords  []string
	Embedding []float32 // added Arc 2: for RAG pipeline
	Err       error
	Retries   int       // added Arc 2: how many retries were needed
}

// ProcessingStage identifies which pipeline stage produced a result.
type ProcessingStage string

const (
	StageScrape    ProcessingStage = "scrape"
	StageClean     ProcessingStage = "clean"
	StageEmbed     ProcessingStage = "embed"
	StageSummarise ProcessingStage = "summarise"
	StageStore     ProcessingStage = "store"
)

// StageResult carries a result through a multi-stage pipeline.
type StageResult struct {
	Article Article
	Stage   ProcessingStage
	Err     error
}
