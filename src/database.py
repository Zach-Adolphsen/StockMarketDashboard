import os

import pandas as pd
import psycopg2

def get_connection():
    db_conn = os.getenv("SUPABASE_URL")
    if not db_conn:
        raise Exception("SUPABASE_URL not found in environment variables")
    return psycopg2.connect(db_conn)

def load_data(symbol_data: pd.DataFrame, symbol_info: pd.DataFrame):
    


    return None