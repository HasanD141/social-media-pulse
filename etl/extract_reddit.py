import json
import time
from pathlib import Path
from typing import List, Dict, Optional

import requests

DATA_RAW_DIR = Path("data/raw")
DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.reddit.com"

HEADERS = {
    "User-Agent": "social-media-pulse-datascience-project by /u/ds_project123",
}


# ---------- 1. Feed-scanning approach for posts ----------

def fetch_ai_posts_by_scanning_feed(
    subreddit_name: str,
    keywords: List[str],
    max_posts: int = 1000,
    page_limit: int = 100,
) -> List[Dict]:
    """
    Scan the /new listing of a subreddit and filter posts locally by keywords.

    :param subreddit_name: e.g. 'technology'
    :param keywords: list of keywords to search in title + selftext (lowercased)
    :param max_posts: target number of matching posts
    :param page_limit: number of posts per listing page (max 100)
    """
    url = f"{BASE_URL}/r/{subreddit_name}/new.json"
    params = {"limit": page_limit}
    after: Optional[str] = None

    collected: List[Dict] = []
    keywords = [k.lower() for k in keywords]

    print(f"Scanning r/{subreddit_name}/new.json for posts containing: {keywords}")

    while len(collected) < max_posts:
        if after:
            params["after"] = after

        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code != 200:
            print(f"Request failed with status {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json().get("data", {})
        children = data.get("children", [])
        after = data.get("after")

        if not children:
            print("No more posts in listing.")
            break

        for child in children:
            d = child.get("data", {})
            title = d.get("title") or ""
            selftext = d.get("selftext") or ""
            text = (title + " " + selftext).lower()

            if any(kw in text for kw in keywords):
                collected.append(
                    {
                        "id": d.get("id"),
                        "subreddit": d.get("subreddit"),
                        "title": title,
                        "selftext": selftext,
                        "score": d.get("score"),
                        "num_comments": d.get("num_comments"),
                        "created_utc": d.get("created_utc"),
                        "author": d.get("author"),
                        "url": d.get("url"),
                        "permalink": f"{BASE_URL}{d.get('permalink')}",
                        "over_18": d.get("over_18"),
                        "upvote_ratio": d.get("upvote_ratio"),
                    }
                )

                if len(collected) >= max_posts:
                    break

        print(f"Collected {len(collected)} matching posts so far...")

        if not after:
            print("Reached the end of the /new listing (no 'after' cursor).")
            break

        time.sleep(2)  # be polite with listing requests

    print(f"Finished scanning. Collected {len(collected)} AI-related posts.")
    return collected


# ---------- 2. Comment flattening ----------

def flatten_comments_tree(comments_data: List[Dict], post_id: str) -> List[Dict]:
    """
    Flatten a Reddit comments tree (from the comments JSON endpoint)
    into a list of simple comment records.
    """
    flattened: List[Dict] = []

    def _walk(node_list: List[Dict]):
        for node in node_list:
            kind = node.get("kind")
            if kind != "t1":  # t1 = comment
                continue

            d = node.get("data", {})
            flattened.append(
                {
                    "post_id": post_id,
                    "comment_id": d.get("id"),
                    "parent_id": d.get("parent_id"),
                    "author": d.get("author"),
                    "body": d.get("body"),
                    "score": d.get("score"),
                    "created_utc": d.get("created_utc"),
                }
            )

            # Replies may be another listing with children
            replies = d.get("replies")
            if isinstance(replies, dict):
                replies_data = replies.get("data", {}).get("children", [])
                _walk(replies_data)

    _walk(comments_data)
    return flattened


# ---------- 3. Comment fetching for multiple posts ----------

def fetch_comments_for_posts(
    posts: List[Dict],
    max_comments_per_post: int = 20,
    max_posts_with_comments: int = 250,
) -> List[Dict]:
    """
    Fetch comments for at most `max_posts_with_comments` posts.

    - Limits comments per post to keep payload small.
    - Sleeps between requests to reduce rate limiting.
    - Stops if Reddit returns 429 (Too Many Requests).
    """
    all_comments: List[Dict] = []

    posts_subset = posts[:max_posts_with_comments]
    total = len(posts_subset)
    print(f"Fetching comments for at most {total} posts...")

    for i, post in enumerate(posts_subset, start=1):
        post_id = post.get("id")
        if not post_id:
            continue

        print(f"[{i}/{total}] Fetching comments for post {post_id}...")

        url = f"{BASE_URL}/comments/{post_id}.json"
        params = {
            "limit": max_comments_per_post,
            "sort": "top",
        }

        resp = requests.get(url, headers=HEADERS, params=params)
        if resp.status_code == 429:
            print("Hit rate limit (429). Stopping further comment requests.")
            break
        if resp.status_code != 200:
            print(f"Comments request for post {post_id} failed with {resp.status_code}")
            continue

        data = resp.json()
        if not isinstance(data, list) or len(data) < 2:
            print("  -> Unexpected comments structure, skipping.")
            continue

        comments_listing = data[1].get("data", {}).get("children", [])
        flat = flatten_comments_tree(comments_listing, post_id=post_id)

        # Hard cap per post
        if len(flat) > max_comments_per_post:
            flat = flat[:max_comments_per_post]

        print(f"  -> Got {len(flat)} comments.")
        all_comments.extend(flat)

        # Short sleep to reduce rate limiting but not take forever
        time.sleep(1)

    print(f"Total flattened comments collected: {len(all_comments)}")
    return all_comments


# ---------- 4. Utility to save JSON ----------

def save_json(data, filename: str):
    filepath = DATA_RAW_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(data)} records to {filepath}")


# ---------- 5. Main script ----------

if __name__ == "__main__":
    # 1) Collect AI-related posts by scanning the /new feed
    ai_keywords = [
        "ai",
        "artificial intelligence",
        "openai",
        "chatgpt",
        "gpt-4",
        "gpt4",
        "machine learning",
        "deep learning",
    ]

    posts = fetch_ai_posts_by_scanning_feed(
        subreddit_name="technology",
        keywords=ai_keywords,
        max_posts=1000,  # target for posts
        page_limit=100,
    )
    save_json(posts, "technology_ai_posts.json")

    # 2) Collect comments for up to 250 posts, with up to 20 comments each
    comments = fetch_comments_for_posts(
        posts,
        max_comments_per_post=20,
        max_posts_with_comments=250,
    )
    save_json(comments, "technology_ai_comments.json")
