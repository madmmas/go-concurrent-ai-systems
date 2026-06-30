package model

// Article represents a news article ingested by the platform.
type Article struct {
	ID      int
	Title   string
	Content string
}

// AIResult holds the output of all AI tasks run against a single article.
type AIResult struct {
	ArticleID int
	Summary   string
	Sentiment string
	Keywords  []string
}
