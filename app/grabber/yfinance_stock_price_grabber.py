# Copyright 2019 LI,JIE-YING. All rights reserved.
import yfinance as yf


def grab_stock_price_by_range(stock_number, start, end):
    return yf.download(stock_number, start=start, end=end)
