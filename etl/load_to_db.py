from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from pymongo import MongoClient

PROCESSED_DIR = Path("data/processed")


def get_env(name: str, default=None):
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable {name} is not set.")
    return value


def load_env():
    # Load from config/.env
    env_path = Path("config/.env")
    if env_path.exists():
        load_dotenv(env_path)
    else:
        print("Warning: config/.env not found – relying on system environment variables.")


# ---------- Postgres helpers ----------

def get_postgres_engine() -> Engine:
    host = get_env("POSTGRES_HOST")
    port = get_env("POSTGRES_PORT")
    db = get_env("POSTGRES_DB")
    user = get_env("POSTGRES_USER")
    password = get_env("POSTGRES_PASSWORD")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url)
    return engine


def create_posts_table(engine: Engine):
    create_table_sql = """
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
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    print("Ensured 'posts' table exists in Postgres.")


def load_posts_to_postgres(engine: Engine, posts_df: pd.DataFrame, if_exists: str = "append"):
    # Keep only columns that match the table definition
    columns = [
        "id",
        "subreddit",
        "title",
        "selftext",
        "score",
        "num_comments",
        "created_utc",
        "created_date",
        "author",
        "url",
        "permalink",
        "over_18",
        "upvote_ratio",
        "title_clean",
        "selftext_clean",
        "text_combined_clean",
    ]
    df = posts_df[columns].copy()

    # Write using pandas to_sql (respects primary key if we handle conflicts manually later)
    df.to_sql("posts", engine, if_exists=if_exists, index=False, method="multi")
    print(f"Inserted {len(df)} posts into Postgres.")


# ---------- MongoDB helpers (for comments) ----------

def get_mongo_collection():
    mongo_uri = get_env("MONGO_URI")
    mongo_db_name = get_env("MONGO_DB")
    comments_collection_name = get_env("MONGO_COMMENTS_COLLECTION", "comments")

    client = MongoClient(mongo_uri)
    db = client[mongo_db_name]
    collection = db[comments_collection_name]
    return collection

def load_comments_to_mongo(comments_df: pd.DataFrame):
    if comments_df.empty:
        print("Comments DataFrame is empty. Skipping MongoDB insertion.")
        return

    # Drop any existing _id column (e.g., if data was exported from Mongo before)
    if "_id" in comments_df.columns:
        comments_df = comments_df.drop(columns=["_id"])

    # Ensure created_utc is a normal datetime (Mongo can handle datetime.datetime)
    if "created_utc" in comments_df.columns:
        comments_df["created_utc"] = pd.to_datetime(comments_df["created_utc"]).dt.tz_localize(None)

    # Convert created_date (datetime.date) to string, since Mongo can't store date objects directly
    if "created_date" in comments_df.columns:
        comments_df["created_date"] = comments_df["created_date"].astype(str)

    collection = get_mongo_collection()

    # Convert DataFrame rows to dicts
    records = comments_df.to_dict(orient="records")
    if not records:
        print("No comment records to insert.")
        return

    result = collection.insert_many(records)
    print(f"Inserted {len(result.inserted_ids)} comments into MongoDB collection '{collection.name}'.")

# ---------- Main ----------

def main():
    load_env()

    posts_path = PROCESSED_DIR / "posts_clean.parquet"
    comments_path = PROCESSED_DIR / "comments_clean.parquet"

    if not posts_path.exists():
        raise FileNotFoundError(f"Processed posts file not found: {posts_path}")

    print(f"Loading posts from {posts_path} ...")
    posts_df = pd.read_parquet(posts_path)
    print(f"Posts shape: {posts_df.shape}")

    # Postgres: create table + insert posts skipped not to load twice
   # engine = get_postgres_engine()
    #create_posts_table(engine)
    #load_posts_to_postgres(engine, posts_df, if_exists="append")

    # MongoDB: comments
    if comments_path.exists():
        print(f"Loading comments from {comments_path} ...")
        comments_df = pd.read_parquet(comments_path)
        print(f"Comments shape: {comments_df.shape}")
        load_comments_to_mongo(comments_df)
    else:
        print(f"No processed comments file at {comments_path}. Skipping MongoDB insertion.")


if __name__ == "__main__":
    main()
