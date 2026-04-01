import yfinance as yf
import pandas as pd

# Download Disney stock data (2015–2020)
dis = yf.download("DIS", start="2015-01-01", end="2020-12-31")

# Calculate daily returns using Adjusted Close
dis['Daily Return'] = dis['Close'].pct_change()

dis.to_csv("disney_stock_2015_2020.csv ", index=True)