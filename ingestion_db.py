import pandas as pd
from sqlalchemy import create_engine
import os
import logging
import time
# Configure logging
logging.basicConfig(
    filename='logs/ingestion.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)
engine = create_engine('sqlite:///inventory.db')
def ingest_db(df, table_name, engine):
    # Ingest the DataFrame into the database
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Table '{table_name}' ingested successfully.")
    engine = create_engine('sqlite:///inventory.db')

def load_raw_data():
    # Load raw data from CSV files in the 'data' directory
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file :
            df = pd.read_csv(f'data/{file}')
            logging.info(f"Ingesting {file} in db")
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info("All files ingested successfully.")
    logging.info(f"Total time taken to ingest all files: {total_time:.2f} seconds")

if __name__ == "__main__":
    load_raw_data()
    
