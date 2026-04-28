import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from scipy import stats
import numpy as np
import os

os.makedirs("plots", exist_ok=True)  # saves all plots to a /plots folder
sns.set_theme(style="whitegrid")
plt.rcParams["figure.dpi"] = 150

df = pd.read_csv("merged_sentiment_stock.csv", parse_dates=["date"])
df.columns = [col.lower().replace(" ", "_") for col in df.columns]
df = df.sort_values("date").reset_index(drop=True)


# PLOT 1: Sentiment vs Stock Price Over Time
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

# sentiment
ax1.plot(df["date"], df["avg_compound"], color="steelblue", linewidth=0.8, label="Avg Sentiment")
ax1.plot(df["date"], df["weighted_compound"], color="orange", linewidth=0.8, alpha=0.7, label="Weighted Sentiment")
ax1.axhline(0, color="black", linewidth=0.5, linestyle="--")
ax1.set_ylabel("Sentiment Score")
ax1.set_title("Reddit Sentiment vs Disney Stock Price (2015–2020)")
ax1.legend(loc="upper left")

# stock price
ax2.plot(df["date"], df["close"], color="green", linewidth=0.8, label="DIS Close Price")
ax2.set_ylabel("Stock Price (USD)")
ax2.set_xlabel("Date")
ax2.legend(loc="upper left")
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

plt.tight_layout()
plt.savefig("plots/1_sentiment_vs_price.png")
plt.close()

# PLOT 2: Lag Analysis
lags = range(0, 8)  # 0 to 7 days
correlations = []

for lag in lags:
    shifted = df["avg_compound"].shift(lag)
    corr, pval = stats.pearsonr(
        shifted.dropna(),
        df["daily_return"].loc[shifted.dropna().index]
    )
    correlations.append({"lag": lag, "correlation": corr, "pvalue": pval})

lag_df = pd.DataFrame(correlations)

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(lag_df["lag"], lag_df["correlation"], color=[
    "steelblue" if p < 0.05 else "lightgray" for p in lag_df["pvalue"]
])
ax.axhline(0, color="black", linewidth=0.5)
ax.set_xlabel("Lag (days)")
ax.set_ylabel("Pearson Correlation")
ax.set_title("Lag Analysis — Sentiment vs Daily Return\n(blue = statistically significant p<0.05)")
ax.set_xticks(lag_df["lag"])
plt.tight_layout()
plt.savefig("plots/3_lag_analysis.png")
plt.close()

# PLOT 3: Event Study 
events = {
    "Force Awakens":      "2015-12-18",
    "Civil War":          "2016-05-06",
    "Rogue One":          "2016-12-16",
    "Beauty & Beast":     "2017-03-17",
    "Last Jedi":          "2017-12-15",
    "Black Panther":      "2018-02-16",
    "Infinity War":       "2018-04-27",
    "Fox Acquisition":    "2019-03-20",
    "Endgame":            "2019-04-26",
    "Disney+ Launch":     "2019-11-12",
    "Iger Resignation":   "2020-02-25",
    "COVID Shutdown":     "2020-03-16",
}

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), sharex=True)

ax1.plot(df["date"], df["avg_compound"], color="steelblue", linewidth=0.8)
ax1.axhline(0, color="black", linewidth=0.5, linestyle="--")
ax1.set_ylabel("Avg Sentiment Score")
ax1.set_title("Event Study — Key Disney Events (2015–2020)")

ax2.plot(df["date"], df["close"], color="green", linewidth=0.8)
ax2.set_ylabel("Stock Price (USD)")
ax2.set_xlabel("Date")

for event, date in events.items():
    event_date = pd.Timestamp(date)
    ax1.axvline(event_date, color="red", linewidth=0.7, linestyle="--", alpha=0.6)
    ax2.axvline(event_date, color="red", linewidth=0.7, linestyle="--", alpha=0.6)
    ax1.text(event_date, ax1.get_ylim()[1] * 0.95, event,
             rotation=90, fontsize=6, color="red", va="top")

plt.tight_layout()
plt.savefig("plots/4_event_study.png")
plt.close()

# extra plot for comparison to recent times

# pull recent data
recent = yf.download("DIS", start="2021-01-01", end="2025-04-25")
recent = recent.reset_index()
recent.columns = [col[0].lower() if isinstance(col, tuple) else col.lower() for col in recent.columns]
recent["date"] = pd.to_datetime(recent["date"])
recent["daily_return"] = recent["close"].pct_change()

# plot both periods side by side
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 5))

# 2015-2020
ax1.plot(df["date"], df["close"], color="steelblue", linewidth=0.8)
ax1.set_title("DIS Stock Price 2015–2020")
ax1.set_ylabel("Price (USD)")
ax1.set_xlabel("Date")
ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

# 2021-2025
ax2.plot(recent["date"], recent["close"], color="orange", linewidth=0.8)
ax2.set_title("DIS Stock Price 2021–2025")
ax2.set_ylabel("Price (USD)")
ax2.set_xlabel("Date")
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

# add key recent events
recent_events = {
    "Chapek fired/Iger returns":     "2022-11-20",
    "Strikes (WGA)":    "2023-05-02",
    "Kimmel controversy":  "2025-09-17",
    "Disney+ profitable":    "2024-02-07",
}

for event, date in recent_events.items():
    event_date = pd.Timestamp(date)
    ax2.axvline(event_date, color="red", linewidth=0.7, linestyle="--", alpha=0.6)
    ax2.text(event_date, ax2.get_ylim()[1] * 0.95, event,
             rotation=90, fontsize=6, color="red", va="top")

plt.tight_layout()
plt.savefig("plots/2_current_stock_comparison.png")
plt.close()

print("\nAll plots saved to /plots folder")