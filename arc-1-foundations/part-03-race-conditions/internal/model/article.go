package model

// Article represents a news article ingested by the platform.
// URL is added in Part 3 as a cache key for the RWMutex demo —
// duplicate URLs are served from cache without a second LLM call.
type Article struct {
	ID      int
	Title   string
	Content string
	URL     string // source URL; empty string = unique article (no caching)
}

// AIResult holds the output of all AI tasks run against a single article.
type AIResult struct {
	ArticleID int
	Summary   string
	Sentiment string
	Keywords  []string
}
