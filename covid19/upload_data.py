import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

def upload_data(engine, csv_folder):
    # Table name and schema name in the database
    table_name = 'raw_tweets'
    schema_name = 'public'

    # Insert data from csv files to database using dataframe
    for filename in os.listdir(csv_folder):
        print(filename)
        if filename.endswith('.CSV'):
            date_str = filename.split(' ')[0]
            date = datetime.strptime(date_str, '%Y-%m-%d').date()
            df = pd.read_csv(os.path.join(csv_folder, filename))
            df['tweet_date'] = date
            df.to_sql(name=table_name, schema=schema_name, con=engine, if_exists='append', index=False)

if __name__ == "__main__":
    load_dotenv()
    csv_folder = os.getenv('CSV_FOLDER')
    postgre_connection_string = os.getenv('POSTGRE_CONNECTION_STRING')
    # Create connection with database postgresql
    engine = create_engine(postgre_connection_string)
    try:
        upload_data(engine, csv_folder)
    finally:
        engine.dispose()