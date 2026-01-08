import os
import time

import requests
from dotenv import load_dotenv
import pandas as pd

from src.database import load_data

symbols: list[str] = ["AAPL", "MSFT", "GOOG", "IBM", "TSLA"]

def get_stock_data(symbol: str):
    url : str = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"
    response = requests.get(url)
    return response.json()

def transform_symbol_data(raw_data) -> pd.DataFrame:
    """
        Handles the transformation of the raw data into a desired format
        Scope of this function is the Time Series (Daily) data
    """

    try:
        nested_data = raw_data["Time Series (Daily)"]
    except KeyError:
        raise KeyError(
            f"Error getting Time Series data: {raw_data.head()}"
        )

    df = pd.DataFrame.from_dict(nested_data, orient="index")
    df.index = pd.to_datetime(df.index)
    df = df.apply(pd.to_numeric)
    df = df.round(2)

    df.columns = df.columns.str.replace(r'^\d+\.\s+', '', regex=True)
    df.index.name = "date"
    df = df.reset_index()

    df['date'] = pd.to_datetime(df['date']).dt.date

    df.drop(columns=["volume"], inplace=True)
    return df

def transform_symbol_meta_data(raw_data) -> pd.DataFrame:

    try:
        nested_data = raw_data["Meta Data"]
    except KeyError:
        raise KeyError(
            f"Error getting Symbol Meta Data: {raw_data.head()}"
        )

    df = pd.DataFrame(nested_data, index=[0])

    df.columns = df.columns.str.replace(r'^\d+\.\s+', '', regex=True)
    df.columns = df.columns.str.replace(r'\s+', '_', regex=True)
    df.columns = df.columns.str.lower()

    df.drop(columns=['information', 'output_size', 'time_zone'], inplace=True)

    df['last_refreshed'] = pd.to_datetime(df['last_refreshed']).dt.date
    return df


if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("API_KEY")

    for s in symbols:
        data = get_stock_data(s)
        symbol_data = transform_symbol_data(data)
        print(symbol_data)
        symbol_info = transform_symbol_meta_data(data)
        print(symbol_info)
        load_data(symbol_info, symbol_data)

