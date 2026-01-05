import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def get_connection():
    db_conn = os.getenv("SUPABASE_URL")
    if not db_conn:
        raise Exception("SUPABASE_URL not found in environment variables")
    return psycopg2.connect(db_conn)

def get_symbols_from_db():
    with get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT symbol_name FROM symbols;")
            current_symbols = cursor.fetchall()  # returns a tuple
            current_symbols = [symbol[0] for symbol in current_symbols]
            print("Current Symbols: " + str(current_symbols))
            return current_symbols
        except Exception as e:
            print("Error getting symbols " + str(e))

def insert_symbol_into_db(symbol, last_refreshed, first_time):
    with get_connection() as conn:
        cursor = conn.cursor()
        if first_time:
            print("Inserting symbol " + symbol + " into database")
            cursor.execute("""
                           INSERT INTO symbols (symbol_name, last_refreshed)
                           VALUES (%s, %s);
                           """,
                           (symbol, last_refreshed))
            conn.commit()
        else:
            print("Updating symbol " + symbol + " last refreshed date with " + str(last_refreshed))
            cursor.execute("""
                           UPDATE symbols
                           SET last_refreshed = %s
                           WHERE symbol_name = %s;
                           """,
                           (last_refreshed, symbol))
            conn.commit()

def insert_symbol_data(symbol_data: pd.DataFrame, symbol: str):
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
                       SELECT symbol_id
                       FROM symbols
                       WHERE symbol_name = %s;
                       """, (symbol,))
        symbol_id = cursor.fetchone()[0]
        print("Symbol ID of " + symbol + " is: " + str(symbol_id))

        temp_df = symbol_data.copy().assign(symbol_id=symbol_id)
        final_df = temp_df[['symbol_id', 'date', 'open', 'high', 'low', 'close']]

        data_to_insert = final_df.to_records(index=False).tolist()
        print("Inserting Data for symbol " + symbol + ": " + str(len(data_to_insert)))
        execute_values(cursor,
                  """
                            INSERT INTO symbol_data (symbol_id, date, open, high, low, close)
                            VALUES %s ON CONFLICT (symbol_id, date) DO NOTHING;
                       """,
                       data_to_insert)
        conn.commit()


def load_data(symbol_info: pd.DataFrame, symbol_data: pd.DataFrame):
    symbol = symbol_info['symbol'][0]
    print("Symbol data type of " + symbol + " is: " + str(type(symbol)))
    last_refreshed = symbol_info['last_refreshed'][0]
    print("Last refreshed data type of " + str(last_refreshed) + " is: " + str(type(last_refreshed)))

    current_symbols = get_symbols_from_db()

    if symbol not in current_symbols:
        try:
            insert_symbol_into_db(symbol, last_refreshed, True)
        except Exception as e:
            print("=" * 50)
            print("ERROR Inserting data: " + str(e))
            print("=" * 50)
    else:
        try:
            insert_symbol_into_db(symbol, last_refreshed, False)
        except Exception as e:
            print("=" * 50)
            print("ERROR Updating symbol meta data: " + str(e))
            print("=" * 50)

    try:
        insert_symbol_data(symbol_data, symbol)
    except Exception as e:
        print("=" * 50)
        print("ERROR Inserting symbol data: " + str(e))
        print("=" * 50)