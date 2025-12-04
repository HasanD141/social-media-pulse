import json
import re
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ----------------------------
# 1. Load your actual data file
# ----------------------------

INPUT_FILE = "/Users/hadishehade/data/raw/technology_ai_comments.json"
OUTPUT_FILE = "technology_ai_comments_sentiment.json"

with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"[+] Loaded {len(data)} comments from {INPUT_FILE}")

# ----------------------------
# 2. Simple text cleaner
# ----------------------------

def clean_text(text):
    if not isinstance(text, str):
        return ""

    text = text.lower()

    # Remove URLs
    text = re.sub(r'https?://\S+', '', text)

    # Remove markdown formatting
    text = re.sub(r'[\*_`>#~\[\]\(\)]', ' ', text)

    # Remove non-alphanumeric characters (except spaces)
    text = re.sub(r'[^a-z0-9\s]', ' ', text)

    # Collapse extra spaces
    text = re.sub(r'\s+', ' ', text).strip()

    return text


# ----------------------------
# 3. Sentiment Analyzer
# ----------------------------

analyzer = SentimentIntensityAnalyzer()

processed = []

for entry in data:
    text = entry.get("body") or entry.get("comment") or ""

    cleaned = clean_text(text)
    sentiment = analyzer.polarity_scores(cleaned)

    # Add new fields for later analysis
    entry["clean_text"] = cleaned
    entry["sentiment_neg"] = sentiment["neg"]
    entry["sentiment_neu"] = sentiment["neu"]
    entry["sentiment_pos"] = sentiment["pos"]
    entry["sentiment_compound"] = sentiment["compound"]

    processed.append(entry)

print(f"[+] Sentiment added to {len(processed)} comments")

# ----------------------------
# 4. Save the enriched data
# ----------------------------

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(processed, f, indent=4)

print(f"[✓] Saved sentiment results to {OUTPUT_FILE}")
