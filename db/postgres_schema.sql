CREATE TABLE IF NOT EXISTS posts (
    id TEXT PRIMARY KEY,
    subreddit TEXT,
    title TEXT,
    selftext TEXT,
    score INTEGER,
    num_comments INTEGER,
    created_utc TIMESTAMPTZ,
    created_date DATE,
    author TEXT,
    url TEXT,
    permalink TEXT,
    over_18 BOOLEAN,
    upvote_ratio REAL,
    title_clean TEXT,
    selftext_clean TEXT,
    text_combined_clean TEXT
);
