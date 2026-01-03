import os
import time
import requests
from dotenv import load_dotenv
import pandas as pd

symbols: list[str] = ["AAPL", "MSFT", "GOOG"]

def get_stock_data(symbol: str):
    url : str = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)
    return response.json()

def transform_symbol_data(symbol: str) -> pd.DataFrame:
    '''
    Handles the transformation of the raw data into a desired format
    Scope of this function is the Time Series (Daily) data
    :return:
    '''
    raw_data = get_stock_data(symbol)

    nested_data = raw_data["Time Series (Daily)"]
    df = pd.DataFrame.from_dict(nested_data, orient="index")
    df.index = pd.to_datetime(df.index)
    df = df.apply(pd.to_numeric)

    df.columns = df.columns.str.replace(r'^\d+\.\s+', '', regex=True)
    df.index.name = "date"
    df = df.reset_index()

    df['date'] = pd.to_datetime(df['date'])

    df.drop(columns=["volume"], inplace=True)
    return df

def transform_symbol_meta_data(symbol: str) -> pd.DataFrame:
    raw_data = get_stock_data(symbol)
    nested_data = raw_data["Meta Data"]
    df = pd.DataFrame(nested_data, index=[0])

    df.columns = df.columns.str.replace(r'^\d+\.\s+', '', regex=True)
    df.columns = df.columns.str.replace(r'\s+', '_', regex=True)
    df.columns = df.columns.str.lower()

    df.drop(columns=['information', 'output_size', 'time_zone'], inplace=True)

    df['last_refreshed'] = pd.to_datetime(df['last_refreshed'])
    return df


if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("API_KEY")

    symbol_data = transform_symbol_data("AAPL")
    print(symbol_data)
    time.sleep(1)
    symbol_info = transform_symbol_meta_data("AAPL")
    print(symbol_info)


