import pandas as pd
import nltk
import glob
import os
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from tqdm import tqdm

nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('stopwords')

# --- LOAD ALL CSVs ---
csv_files = glob.glob("reddit_data/backup_*.csv")  # picks up all your backup CSVs
print(f"Found {len(csv_files)} CSV files: {csv_files}")

df = pd.concat(
    [pd.read_csv(f) for f in csv_files],
    ignore_index=True
)
print(f"Total records loaded: {len(df)}")

# --- DEDUPLICATE ---
df.drop_duplicates(subset=["type", "id"], inplace=True)
print(f"After deduplication: {len(df)} records")

# --- CLEAN TEXT ---
stop_words = set(stopwords.words('english'))

def clean_text(text):
    if not isinstance(text, str) or text.strip() == "":
        return ""
    if text.strip() in ["[deleted]", "[removed]"]:
        return ""
    tokens = word_tokenize(text.lower())
    tokens = [t for t in tokens if t.isalpha()]
    tokens = [t for t in tokens if t not in stop_words]
    tokens = [t for t in tokens if len(t) > 2]
    return " ".join(tokens)

print("Cleaning text...")
df["cleaned_text"] = df["text"].apply(clean_text)
df = df[df["cleaned_text"].str.strip() != ""].reset_index(drop=True)
print(f"After cleaning: {len(df)} records")

# --- CONVERT TIMESTAMP ---
df["date"] = pd.to_datetime(df["created_utc"], unit="s").dt.date

# --- SENTIMENT SCORING ---
analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    scores = analyzer.polarity_scores(text)
    return pd.Series({
        "vader_neg":      scores["neg"],
        "vader_neu":      scores["neu"],
        "vader_pos":      scores["pos"],
        "vader_compound": scores["compound"]
    })

print("Running sentiment analysis...")
results = []
for text in tqdm(df["cleaned_text"], desc="Scoring"):
    results.append(get_sentiment(text))
df[["vader_neg", "vader_neu", "vader_pos", "vader_compound"]] = pd.DataFrame(results).values

# --- LABEL SENTIMENT ---
def label_sentiment(compound):
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    else:
        return "neutral"

df["sentiment_label"] = df["vader_compound"].apply(label_sentiment)

# --- SUBREDDIT TYPE ---
finance_subs = {"wallstreetbets", "investing", "stocks", "stockmarket"}

df["sentiment_type"] = df["subreddit"].apply(
    lambda s: "investor" if str(s).lower() in finance_subs else "consumer"
)

# --- AGGREGATE: DAILY AVERAGE ---
daily_avg = (
    df.groupby("date")
    .agg(
        avg_compound=("vader_compound", "mean"),
        post_count=("vader_compound", "count")
    )
    .reset_index()
)

# --- AGGREGATE: WEIGHTED BY UPVOTES ---
df["weight"] = df["score"].clip(lower=1)
daily_weighted = (
    df.groupby("date")
    .apply(lambda x: (x["vader_compound"] * x["weight"]).sum() / x["weight"].sum())
    .reset_index()
    .rename(columns={0: "weighted_compound"})
)

# --- AGGREGATE: CONSUMER VS INVESTOR ---
daily_split = (
    df.groupby(["date", "sentiment_type"])
    .agg(avg_compound=("vader_compound", "mean"))
    .reset_index()
    .pivot(index="date", columns="sentiment_type", values="avg_compound")
    .reset_index()
    .rename(columns={"consumer": "consumer_sentiment", "investor": "investor_sentiment"})
)

# --- SAVE ---
print("Saving...")
df.to_csv("scored_raw.csv", index=False)
daily_avg.to_csv("scored_daily_avg.csv", index=False)
daily_weighted.to_csv("scored_daily_weighted.csv", index=False)
daily_split.to_csv("scored_consumer_vs_investor.csv", index=False)

print(f"Done. {len(df)} records scored.")
print("Files saved:")
print("  - scored_raw.csv")
print("  - scored_daily_avg.csv")
print("  - scored_daily_weighted.csv")
print("  - scored_consumer_vs_investor.csv")