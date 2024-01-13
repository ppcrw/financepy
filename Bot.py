import pandas_datareader as pdr
import datetime
from thaifin import Stock
import talib
import numpy as np
import itertools as it
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')


# Visually Show The Stock Price(s)
# Create the title
title = 'Close Price History    '
# Get the stocks
my_stocks = df


def __init__(self, _my_stock):
    self.my_stock = _my_stock


def price_plot(self):
    # Create and plot the graph
    plt.figure(figsize=(12.2, 4.5))  # width = 12.2in, height = 4.5
    # plt.plot( X-Axis , Y-Axis, line_width, alpha_for_blending,  label)
    plt.plot(self.my_stocks['Close'],  label='Close')
    plt.xticks(rotation=45)
    plt.title(title)
    plt.xlabel('Date', fontsize=18)
    plt.ylabel('Price THB (฿)', fontsize=18)
    plt.show()


# Calculate the MACD and Signal Line indicators
# Calculate the Short Term Exponential Moving Average
# AKA Fast moving average
ShortEMA = df.Close.ewm(span=12, adjust=False).mean()
# Calculate the Long Term Exponential Moving Average
LongEMA = df.Close.ewm(span=26, adjust=False).mean()  # AKA Slow moving average
# Calculate the Moving Average Convergence/Divergence (MACD)
MACD = ShortEMA - LongEMA
# Calcualte the signal line
signal = MACD.ewm(span=9, adjust=False).mean()
# Plot the chart
plt.figure(figsize=(12.2, 4.5))  # width = 12.2in, height = 4.5
plt.plot(df.index, MACD, label='%s MACD' % (quote), color='red')
plt.plot(df.index, signal, label='Signal Line', color='blue')
plt.xticks(rotation=45)
plt.legend(loc='upper left')
plt.show()
