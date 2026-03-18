import requests
import pandas as pd
import time
from tqdm import tqdm

# using arctic instead of praw ahh
BASE_URL = "https://arctic-shift.photon-reddit.com"

# testing 48 hrs
START_DATE = "2015-01-01"
END_DATE   = "2015-01-02"

QUERIES = [
    "cinderella",
    "stock",
    "Disneyland",
]

SUBREDDITS = [
    "disney",
]

HEADERS = {"User-Agent": "disney-sentiment-research/1.0"}

# fetching
def fetch_posts(query, subreddit):
    posts = []
    after = START_DATE

    while True:
        params = {
            "query": query,          # fixed: was "q", correct param is "query"
            "subreddit": subreddit,
            "after": after,
            "before": END_DATE,
            "limit": 100,
            "sort": "asc",
            "fields": "id,title,selftext,score,created_utc,subreddit"
        }

        try:
            r = requests.get(
                f"{BASE_URL}/api/posts/search",
                params=params,
                headers=HEADERS,
                timeout=30
            )
            r.raise_for_status()
            batch = r.json().get("data", [])
        except Exception as e:
            print(f"  Error fetching posts: {e}")
            break

        if not batch:
            break

        posts.extend(batch)

        if len(batch) < 100:
            break

        # advance timestamp to paginate
        after = str(int(batch[-1]["created_utc"]) + 1)
        time.sleep(0.5)

    return posts


def fetch_comments(post_id, subreddit, query):
    try:
        r = requests.get(
            f"{BASE_URL}/api/comments/tree",
            params={
                "link_id": f"t3_{post_id}",
                "limit": 9999,       # get all comments in one call
            },
            headers=HEADERS,
            timeout=30
        )
        r.raise_for_status()
        comments_raw = r.json().get("data", [])
    except Exception as e:
        print(f"  Error fetching comments for {post_id}: {e}")
        return []

    # flatten the tree (filter out "more" collapsed nodes)
    comments = []
    for c in comments_raw:
        if c.get("kind") == "more":
            continue
        comments.append({
            "type": "comment",
            "id": c.get("id"),
            "text": c.get("body", ""),
            "score": c.get("score"),
            "created_utc": c.get("created_utc"),
            "subreddit": subreddit,
            "query": query
        })
    return comments


# main function: get posts and comments
data = []
seen_post_ids = set()

for query in QUERIES:
    for subreddit in SUBREDDITS:
        print(f"\nFetching: '{query}' in r/{subreddit}")

        posts = fetch_posts(query, subreddit)
        print(f"  → {len(posts)} posts found")

        for post in tqdm(posts):
            post_id = post["id"]

            data.append({
                "type": "post",
                "id": post_id,
                "text": post.get("title", "") + " " + (post.get("selftext") or ""),
                "score": post.get("score"),
                "created_utc": post.get("created_utc"),
                "subreddit": post.get("subreddit"),
                "query": query
            })

            if post_id not in seen_post_ids:
                seen_post_ids.add(post_id)
                comments = fetch_comments(post_id, subreddit, query)
                data.extend(comments)
                time.sleep(0.3)

# --- SAVE ---
df = pd.DataFrame(data)
df.drop_duplicates(subset=["type", "id"], inplace=True)
df.to_csv("disney_reddit_2015_2020.csv", index=False)
print(f"\nDone. {len(df)} total records saved.")