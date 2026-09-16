// Package model defines the core data types for the news intelligence platform.
// Arc 3 adds TokenUsage for cost tracking and ProcessingMetrics for diagnostics.
package model

import "time"

// Article represents a news article ingested by the platform.
type Article struct {
	ID        int
	Title     string
	Content   string
	URL       string
	Source    string
	FetchedAt time.Time
}

// AIResult holds the output of all AI tasks for a single article.
type AIResult struct {
	ArticleID int
	Summary   string
	Sentiment string
	Keywords  []string
	Embedding []float32
	Err       error
	Retries   int
	// Arc 3 additions
	TokensUsed int           // total tokens consumed (Arc 3 Part 25+)
	Duration   time.Duration // end-to-end processing time
}

// ProcessingStage identifies which pipeline stage produced a result.
type ProcessingStage string

const (
	StageScrape    ProcessingStage = "scrape"
	StageClean     ProcessingStage = "clean"
	StageEmbed     ProcessingStage = "embed"
	StageSummarise ProcessingStage = "summarise"
)
