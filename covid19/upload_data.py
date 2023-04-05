import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Create connection with database postgresql
engine = create_engine('postgresql://postgres:password@localhost:5432/postgres')

# CSV folder path
csv_folder = '/home/haiho/data'

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

# Close connection with database
engine.dispose()