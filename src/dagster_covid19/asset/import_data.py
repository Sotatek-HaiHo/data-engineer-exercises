import os
from datetime import datetime
from tempfile import TemporaryDirectory

import pandas as pd
from dagster import asset, AssetIn, io_manager, IOManager, Output, RetryPolicy
from dotenv.main import load_dotenv
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine

load_dotenv()


class MyIOManager(IOManager):
    def handle_output(self, context, obj):
        load_dotenv()
        output_path = os.getenv("DAGSTER_ASSET_TMPDIR")
        i = 1
        for element in obj:
            parquet_file = output_path + "/" + str(i) + context.metadata.get("name")
            element.to_parquet(parquet_file)
            i += 1

    def load_input(self, context):
        tmpdir = os.getenv("DAGSTER_ASSET_TMPDIR")
        i = 1
        df_list = list()
        for element in os.listdir(tmpdir):
            input_path = tmpdir + "/" + str(i) + context.metadata.get("name")
            df = pd.read_parquet(input_path)
            df_list.append(df)
            i += 1
        return df_list


def download_and_extract_dataset():
    temporary_directory = os.getenv("DAGSTER_ASSET_TMPDIR")
    kg = KaggleApi()
    kg.authenticate()
    df_list = list()
    with TemporaryDirectory(dir=temporary_directory) as tmpdir:
        kg.dataset_download_files(
            dataset="smid80/coronavirus-covid19-tweets-early-april",
            path=tmpdir,
            unzip=True,
        )
    for filename in os.listdir(tmpdir):
        print(filename.split(".")[0])
        if filename.endswith(".CSV"):
            date_str = filename.split(" ")[0]
            date = datetime.strptime(date_str, "%Y-%m-%d").date()
            df = pd.read_csv(os.path.join(tmpdir, filename))
            df["tweet_date"] = date
            df.rename({"text": "content"}, axis=1, inplace=True)
            df_list.append(df)
    return Output(df_list, metadata={"name": "covid.parquet"})


@io_manager
def my_io_manager():
    return MyIOManager()


@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    io_manager_key="my_io_manager",
    key_prefix=["raw_tweets"],
    resource_defs={"my_io_manager": my_io_manager},
    metadata={"name": "covid.parquet"},
)
def parquet_files():
    return download_and_extract_dataset()


def upload_data(engine, df):
    table_name = "raw_tweets"
    schema_name = "public"
    df.to_sql(
        name=table_name, schema=schema_name, con=engine, if_exists="append", index=False
    )


@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    key_prefix=["raw_tweets"],
    resource_defs={"my_io_manager": my_io_manager},
    ins={
        "parquet_files": AssetIn(
            key="parquet_files",
            input_manager_key="my_io_manager",
            metadata={"name": "covid.parquet"},
        )
    },
)
def raw_tweets(parquet_files: list):
    postgres_connection_string = os.getenv("POSTGRE_CONNECTION_STRING")
    if postgres_connection_string is None:
        raise Exception("POSTGRE_CONNECTION_STRING is not set")
    else:
        engine = create_engine(postgres_connection_string)
        for df in parquet_files:
            upload_data(engine, df)
