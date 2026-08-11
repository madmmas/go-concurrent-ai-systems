package model

type Article struct {
	ID      int
	Title   string
	Content string
}

type AIResult struct {
	ArticleID int
	Summary   string
	Sentiment string
	Keywords  []string
	Err       error
}
