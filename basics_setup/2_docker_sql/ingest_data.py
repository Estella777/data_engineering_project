# create a table in Postgres first, need to connect to Postgresql first
# insert data in chuncks

import os
import argparse

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from sqlalchemy import text

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.parquet.gz'):
        parquet_name = 'output.parquet.gz'
    else:
        parquet_name = 'output.parquet'

    #download parquet file
    os.system(f"wget {url} -O {parquet_name}")
    

    # use this way to just get df file and get DDL command to create table. To actually load data in chunks, use pyarrow down below
    df = pd.read_parquet(parquet_name)

    chunk_size = 100000

    #user:password@host:portal/database_name
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #only can be used in Pandas, this uses df to generate DDL command used to create a table in Postgres
    create_table_sql = pd.io.sql.get_schema(df, name=table_name, con=engine)

    #create table in Postgresql
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))  # Optional: drop if exists
        conn.execute(text(create_table_sql)) # text: This wraps your raw SQL string in a sqlalchemy.sql.text object, which allows SQLAlchemy to safely execute raw SQL.
        conn.commit()

    #load data in chuncks
    pf = pq.ParquetFile(parquet_name)
    total_rows = pf.metadata.num_rows
    print(f'Total rows in file:{total_rows:,}') # add , to do thousands

    #using pyarrow to load in chunck
    # first Read all data (all row groups) into a PyArrow Table, then slice it
    for i in range(0, total_rows, chunk_size):
        #pf.num_row_groups return number of groups, and range create a list like in a for loop
        table_chunck = pf.read_row_groups(range(pf.num_row_groups)).slice(i, chunk_size)
        df_chunk = table_chunck.to_pandas()
        df_chunk.to_sql(table_name, engine, if_exists='append', index = False)
        print(f'Inserted rows {i+1} to {min(i + chunk_size, total_rows):,}')
    print('Done loading all chunks.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the parquet file')

    args = parser.parse_args()

    main(args)







