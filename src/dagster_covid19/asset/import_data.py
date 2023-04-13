import os
from datetime import datetime
from tempfile import TemporaryDirectory

import pandas as pd
from dagster import asset, RetryPolicy
from dotenv.main import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine


def download_and_extract_dataset_to_database():
    load_dotenv()
    postgres_connection_string = os.getenv("POSTGRE_CONNECTION_STRING")
    temporary_directory = os.getenv("DAGSTER_ASSET_TMPDIR")
    if postgres_connection_string is None:
        raise Exception("POSTGRE_CONNECTION_STRING is not set")
    else:
        engine = create_engine(postgres_connection_string)
        kg = KaggleApi()
        kg.authenticate()
        with TemporaryDirectory(dir=temporary_directory) as tmpdir:
            kg.dataset_download_files(
                dataset="smid80/coronavirus-covid19-tweets-early-april",
                path=tmpdir,
                unzip=True,
            )
            try:
                upload_data(engine, tmpdir)
            finally:
                engine.dispose()


def upload_data(engine, csv_folder):
    # Table name and schema name in the database
    table_name = "raw_tweets"
    schema_name = "public"

    # Insert data from csv files to database using dataframe
    for filename in os.listdir(csv_folder):
        print(filename)
        if filename.endswith(".CSV"):
            date_str = filename.split(" ")[0]
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
            df = pd.read_csv(os.path.join(csv_folder, filename))
            df["tweet_date"] = date
            df.rename({"text": "content"}, axis=1, inplace=True)
            df.to_sql(
                name=table_name,
                schema=schema_name,
                con=engine,
                if_exists="append",
                index=False,
            )


@asset(retry_policy=RetryPolicy(max_retries=3, delay=60), key_prefix=["raw_tweets"])
def raw_tweets():
    return download_and_extract_dataset_to_database()
