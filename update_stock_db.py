from datetime import datetime, timedelta, time
import yfinance as yf
from sqlalchemy import create_engine, text
import pandas as pd
from thaifin import Stock

yf.pdr_override()

def get_all_stock_symbols():
    symbols = [i.lower() + ".bk" for i in Stock.list_symbol()]
    return symbols

def get_last_date(symbol, conn):
    try:
        query = text(f"SELECT `Date` FROM `{symbol}` ORDER BY `Date` DESC LIMIT 1")
        result = conn.execute(query)
        last_date = result.scalar()
        if last_date:
            last_date = pd.to_datetime(last_date).date()
            print(symbol, last_date)
        else:
            print(f'No {symbol} in db')
            last_date = None
    except Exception as e:
        print(f'Error getting last date for {symbol}: {e}')
        last_date = None
    return last_date

def download_stock_data(symbol, start_date, end_date):
    df = yf.download(symbol, start=start_date, end=end_date)
    return df

def update_stock_data(stock_symbols, engine):
    with engine.connect() as conn:
        for symbol in stock_symbols:
            print(symbol)
            # Get the latest date in the symbol's table
            last_date = get_last_date(symbol, conn)
            
            if last_date is None:
                df = download_stock_data(symbol, datetime(2000, 1, 1), datetime.today().date() - timedelta(days=1))
                if df.shape[0] == 0:
                    print(f"{symbol} does not exist in yfin")
                else:
                    print(f"Create {symbol} table in {engine.url.database}")
                    update_database(symbol, df, engine, last_date)
                continue

            if last_date == datetime.today().date():
                print(symbol, " is up-to-date")
                continue

            start_date = last_date + timedelta(days=1)
            if datetime.now().time() < time(18, 0, 0):
                end_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                end_date = datetime.now()
            
            print(symbol, start_date, end_date)
            if start_date == end_date:
                print(symbol, "is up-to-date at ", end_date)
                continue

            df = download_stock_data(symbol, start_date, end_date)
            update_database(symbol, df, engine, last_date)

            print()  # Blank line for separation

def update_database(symbol, df, engine, last_date):
    try:
        if not df.empty and df.index[-1].date() == last_date:
            print(symbol, "is updated")
        else:
            # export to sql
            df.to_sql(symbol, engine, if_exists="append", method='multi')
            print(symbol, "is updated to ", df.index[-1])
    except Exception as e:
        print(f'Error updating {symbol}: {e}')


def update_all(engine):
    stock_symbols = get_all_stock_symbols()
    update_stock_data(stock_symbols, engine)

def main():
    db_file = "bk.db"
    engine = create_engine(f'sqlite:///{db_file}')
    try:
        update_all(engine)
    finally:
        engine.dispose()

if __name__ == "__main__":
    main()
