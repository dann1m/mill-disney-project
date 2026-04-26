import pandas as pd

daily_avg = pd.read_csv("scored_daily_avg.csv")
daily_weighted = pd.read_csv("scored_daily_weighted.csv")
daily_split = pd.read_csv("scored_consumer_vs_investor.csv")
stock = pd.read_csv("disney_stock_2015_2020.csv")

# the dates header didn't match aghh
daily_avg["date"] = pd.to_datetime(daily_avg["date"])
daily_weighted["date"] = pd.to_datetime(daily_weighted["date"])
daily_split["date"] = pd.to_datetime(daily_split["date"])
stock["date"] = pd.to_datetime(stock["date"])

sentiment_all = (
    daily_avg
    .merge(daily_weighted, on="date")
    .merge(daily_split, on="date")
)

merged = pd.merge(sentiment_all, stock, on="date", how="inner")

merged.to_csv("merged_sentiment_stock.csv", index=False)
print(f"Done. {len(merged)} trading days merged.")
print(merged.head())
print(f"\nColumns: {list(merged.columns)}")