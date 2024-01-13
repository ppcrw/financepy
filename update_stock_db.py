from datetime import datetime, timedelta, time
from thaifin import Stock
import yfinance as yf

yf.pdr_override()
import sqlalchemy
import pandas as pd
import numpy as np


def get_all_stock_symbols():
    symbols = [i.lower() + ".bk" for i in Stock.list_symbol()]
    return symbols


def get_last_date(symbol, engine):
    try:
        sql = f"SELECT `Date` FROM `{symbol}` ORDER BY `Date` desc LIMIT 1  "
        last_date = pd.read_sql(sql, engine)["Date"][0]

    except sqlalchemy.exc.ProgrammingError:
        print(symbol, " does not exist.")
        last_date = None

    # print(symbol, last_date)
    return last_date


def update_stock_data(stock_symbols, engine):
    # Get a list of stock symbols for the Thailand market
    for symbol in stock_symbols:
        # Get the latest date in the symbol's table
        last_date = get_last_date(symbol, engine)
        print(last_date)
        # If the latest date is None, there is no existing data for the symbol
        # Set the start date to a fixed date in the past
        if last_date is None:
            print(symbol, "last date none. downloading...")
            df = yf.download(
                symbol,
                start=datetime(2000, 1, 1),
                end=(datetime.today() - timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                ),
            )
            if df.shape[0] == 0:
                print(f"{symbol} does not exist in yfin")
            else:
                print(f"Create {symbol} table in {engine.url.database}")
                df.to_sql(symbol, engine, if_exists="append")
            continue

        if last_date.date() == datetime.today().date():
            print(symbol, " is up-to-date")
            continue

        else:
            # Specify the time frame for historical data
            start_date = last_date + timedelta(
                days=1
            )  # Start from the day after the latest date
            if datetime.now().time() < time(18, 0, 0):
                end_date = (datetime.today()).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            else:
                end_date = datetime.now()
            print(symbol, start_date, end_date)
            if start_date == end_date:
                print(symbol, "is up-to-date at ", end_date)
                continue
            # download the stock data in the symbol's table
            df = yf.download(symbol, start_date, end_date)

            # check if duplicate
            try:
                if df.index[-1] == last_date:
                    print(symbol, "is updated")

                else:
                    # export to sql
                    df.to_sql(symbol, engine, if_exists="append")
                    print(symbol, "is updated to ", df.index[-1])
            except:
                print(symbol, "not found")


def update_all(engine):
    stock_symbols = get_all_stock_symbols()
    update_stock_data(stock_symbols, engine)


# Config
dbname = "bk"
mysqlroot = f"sqlite:///{dbname}.db"
engine = sqlalchemy.create_engine(mysqlroot)
stock_symbols = get_all_stock_symbols()


if __name__ == "__main__":
    # update_all(engine)
    update_stock_data(stock_symbols, engine)
