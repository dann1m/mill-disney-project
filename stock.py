import yfinance as yf
import pandas as pd

# Download Disney stock data (2015–2020)
dis = yf.download("DIS", start="2015-01-01", end="2020-12-31")

print(dis.head())
