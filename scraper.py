import requests
import pandas as pd
import time
from tqdm import tqdm

# using arctic instead of praw ahh
BASE_URL = "https://arctic-shift.photon-reddit.com"

# testing 48 hrs
START_DATE = "2015-01-01"
END_DATE   = "2020-12-31"

QUERIES = [
    # Animated / Pixar
    "Inside Out",
    "Zootopia",
    "Moana",
    "Coco",
    "Incredibles 2",
    "Ralph Breaks the Internet",
    "Toy Story 4",
    "Frozen 2",
    "Good Dinosaur",
    "Finding Dory",
    "Cars 3",
    "Soul Pixar", 
    
    # Live action / Marvel
    "Avengers Age of Ultron",
    "Captain America Civil War",
    "Doctor Strange",
    "Black Panther",
    "Avengers Infinity War",
    "Avengers Endgame",
    "Spider-Man Homecoming",
    "Captain Marvel",
    "Ant-Man",
    "Guardians Galaxy 2",
    "Thor Ragnarok",
    "Ant-Man Wasp",
    "Spider-Man Far From Home",
    
    # Star Wars
    "Force Awakens",
    "Rogue One",
    "Last Jedi",
    "Solo Star Wars",
    "Rise of Skywalker",
    "Mandalorian",    
    
    # Live action remakes
    "Cinderella 2015",
    "Jungle Book 2016",
    "Beauty and the Beast 2017",
    "Aladdin 2019",
    "Lion King 2019",
    "Dumbo 2019",
    "Maleficent Mistress",
    "Mulan 2020",
    "Lady Tramp 2019",
    
    # Other notable films
    "A Wrinkle in Time",
    "Mary Poppins Returns",
    "Christopher Robin",
    "Alice Through Looking Glass",
    "Pete Dragon 2016",
    "Artemis Fowl",
    "Hamilton Disney"

    # Streaming
    "Disney Plus launch",
    "Disney Plus subscription",
    "Disney streaming",
    "Disney Plus vs Netflix",
    
    # Parks
    "Disney World",
    "Disneyland",
    "Star Wars Galaxy Edge",
    "Disney park crowds",
    "Disney park prices",
    
    # CEO
    "Bob Iger",
    "Bob Chapek",
    "Disney CEO",
    "Iger resignation",
    "Iger successor",
    
    # Stock / Business
    "Disney stock",
    "DIS stock",
    "Disney earnings",
    "Disney acquisition",
    "Disney Fox acquisition",
    "21st Century Fox Disney",
    "Disney revenue",
    "Disney subscriber"
]

SUBREDDITS = [
    "pixar"
    # "movies",
    "television",
    "disney",        
    "WaltDisneyWorld",
    "DisneyPlus",
    "entertainment",
    "MarvelStudios",
    "StarWars",
    "wallstreetbets",
    "investing",
    "stocks",
    "StockMarket"
]

HEADERS = {"User-Agent": "disney-sentiment-research/1.0"}

# fetching
def fetch_posts(query, subreddit):
    posts = []
    after = START_DATE

    while True:
        params = {
            "query": query,        
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


seen_post_ids = set()

# with pd.ExcelWriter("disney_reddit_2015_2020.xlsx", engine="openpyxl") as writer:
for query in QUERIES:
        query_data = []

        for subreddit in SUBREDDITS:
            print(f"\nFetching: '{query}' in r/{subreddit}")

            posts = fetch_posts(query, subreddit)
            print(f"  → {len(posts)} posts found")

            if not posts:
                continue

            for post in tqdm(posts, desc=f"r/{subreddit}"):
                post_id = post["id"]

                query_data.append({
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
                    # comments = fetch_comments(post_id, subreddit, query)
                    # query_data.extend(comments)
                    time.sleep(0.3)

        # save everything for this query to one sheet
        if query_data:
            sheet_name = query[:31].replace("/", "").replace("*", "").replace("?", "").replace(":", "").replace("[", "").replace("]", "")
            df_query = pd.DataFrame(query_data)
            csv_name = f"backup_{sheet_name}.csv"
            df_query.to_csv(csv_name, index=False)
            print(f"  → CSV backup saved: {csv_name}")

        else:
            print(f"  → no data found for '{query}', skipping sheet")