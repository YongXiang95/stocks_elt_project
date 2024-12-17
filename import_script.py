import psycopg2
import csv
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')

def import_to_postgresql(market, df):

    hostname = DB_HOST
    database = DB_NAME
    username = DB_USERNAME
    pwd = DB_PASSWORD
    port_id = DB_PORT
    conn = None
    cur= None

    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )
        cur = conn.cursor()
        table_name = market.lower() + '_latest'
        # Create table for each market in postgres
        drop_script = f'DROP TABLE IF EXISTS {table_name}'        
        create_script = f'''CREATE TABLE {table_name} (
                                ticker VARCHAR(30) NOT NULL,
                                volume FLOAT,
                                volume_weighted_avg_price FLOAT,
                                open_price FLOAT,
                                close_price FLOAT,
                                highest_price FLOAT,
                                lowest_price FLOAT,
                                um_timestamp BIGINT,
                                transactions FLOAT,
                                market_close DATE,
                                market VARCHAR(50) NOT NULL,
                                PRIMARY KEY (ticker)
                            ) '''
        cur.execute(drop_script)
        cur.execute(create_script)
        
        # import df for each market into postgres
        columns = df.columns.tolist()  # Get column names from DataFrame
        placeholders = ', '.join(['%s'] * len(columns))  # Create placeholders for each column

        for index, row in df.iterrows():
            # Replace NaN values with None, which will be interpreted as NULL in PostgreSQL
            row = row.where(pd.notna(row), None)
            
            query = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'
            cur.execute(query, tuple(row))

        conn.commit()
        print("successful import to PostgreSQL")
        
    except Exception as error:
        print(error)

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()