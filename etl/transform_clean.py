import json
import re
from pathlib import Path
from typing import Tuple

import pandas as pd

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


URL_PATTERN = re.compile(r"http\S+|www\.\S+")
NON_ALPHANUM = re.compile(r"[^a-zA-Z0-9\s]")


def clean_text(text: str) -> str:
    """Basic text cleaning: remove URLs, lowercase, strip punctuation-type chars."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = URL_PATTERN.sub("", text)
    text = text.lower()
    text = NON_ALPHANUM.sub(" ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def load_and_transform_posts() -> pd.DataFrame:
    posts_path = RAW_DIR / "technology_ai_posts.json"
    if not posts_path.exists():
        raise FileNotFoundError(f"Raw posts file not found: {posts_path}")

    with open(posts_path, "r", encoding="utf-8") as f:
        posts = json.load(f)

    df = pd.DataFrame(posts)

    if df.empty:
        raise ValueError("Posts DataFrame is empty. Did extraction fail?")

    # Parse timestamps
    if "created_utc" in df.columns:
        df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)
        df["created_date"] = df["created_utc"].dt.date

    # Clean text fields
    df["title_clean"] = df["title"].apply(clean_text)
    df["selftext_clean"] = df["selftext"].apply(clean_text)

    # Simple combined text field for later NLP
    df["text_combined_clean"] = (df["title_clean"] + " " + df["selftext_clean"]).str.strip()

    # Make sure id is string
    df["id"] = df["id"].astype(str)

    return df


def load_and_transform_comments() -> pd.DataFrame:
    comments_path = RAW_DIR / "technology_ai_comments.json"
    if not comments_path.exists():
        print(f"No comments file found at {comments_path}. Returning empty DataFrame.")
        return pd.DataFrame()

    with open(comments_path, "r", encoding="utf-8") as f:
        comments = json.load(f)

    df = pd.DataFrame(comments)

    if df.empty:
        print("Comments DataFrame is empty.")
        return df

    # Parse timestamps
    if "created_utc" in df.columns:
        df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)
        df["created_date"] = df["created_utc"].dt.date

    # Clean text field
    df["body_clean"] = df["body"].apply(clean_text)

    # Ensure IDs are strings
    df["post_id"] = df["post_id"].astype(str)
    df["comment_id"] = df["comment_id"].astype(str)

    return df


def main() -> Tuple[pd.DataFrame, pd.DataFrame]:
    print("Loading and transforming posts...")
    posts_df = load_and_transform_posts()
    print(f"Posts shape: {posts_df.shape}")

    print("Loading and transforming comments...")
    comments_df = load_and_transform_comments()
    print(f"Comments shape: {comments_df.shape}")

    posts_out = PROCESSED_DIR / "posts_clean.parquet"
    comments_out = PROCESSED_DIR / "comments_clean.parquet"

    posts_df.to_parquet(posts_out, index=False)
    print(f"Saved cleaned posts to {posts_out}")

    if not comments_df.empty:
        comments_df.to_parquet(comments_out, index=False)
        print(f"Saved cleaned comments to {comments_out}")
    else:
        print("No comments to save.")

    return posts_df, comments_df


if __name__ == "__main__":
    main()
