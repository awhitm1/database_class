import mysql.connector
from dotenv import dotenv_values
import mysql.connector.errorcode
import pandas as pd

TMDB_BEARER_TOKEN, TMDB_API_KEY, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = dotenv_values(".env").values()

def execute_query(query: str, data = {}):
    conn = mysql.connector.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, database=DB_NAME)
    
    try:
        return pd.read_sql(query, conn, index_col='tmdb_id')
    except mysql.connector.errorcode as e:
        print(f"Something went wrong: {e}")
    finally:
        conn.close()

