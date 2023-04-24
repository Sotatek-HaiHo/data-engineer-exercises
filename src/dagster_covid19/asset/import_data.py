import logging
import os
from datetime import datetime
from tempfile import TemporaryDirectory

import pandas as pd
from dagster import asset, AssetIn, Output, RetryPolicy
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine, engine

from src.dagster_covid19.asset.asset_resources import ddl


def _download_and_extract_dataset():
    temporary_directory = os.getenv("DAGSTER_ASSET_TMPDIR")
    kg = KaggleApi()
    kg.authenticate()

    def df_gen():
        with TemporaryDirectory(dir=temporary_directory) as tmpdir:
            kg.dataset_download_files(
                dataset="smid80/coronavirus-covid19-tweets-early-april",
                path=tmpdir,
                unzip=True,
            )
            for filename in os.listdir(tmpdir):
                logging.info(msg=filename.split(".")[0])
                if filename.endswith(".CSV"):
                    date_str = filename.split(" ")[0]
                    date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    df = pd.read_csv(os.path.join(tmpdir, filename))
                    df["tweet_date"] = date
                    df.rename({"text": "content"}, axis=1, inplace=True)
                    yield df

    df_generator = df_gen()
    return Output(df_generator, metadata={"name": "covid.parquet"})


@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    io_manager_key="df_io_manager",
    key_prefix=["raw_tweets"],
    metadata={"name": "covid.parquet"},
)
def parquet_files() -> Output:
    return _download_and_extract_dataset()


def _upload_data(
    engine: engine, df: pd.DataFrame, table_name: str, schema_name: str
) -> None:
    df.to_sql(
        name=table_name,
        schema=schema_name,
        con=engine,
        if_exists="append",
        index=False,
    )


@asset(
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    key_prefix=["raw_tweets"],
    ins={
        "parquet_files": AssetIn(
            key="parquet_files",
            input_manager_key="df_io_manager",
            metadata={"name": "covid.parquet"},
        )
    },
)
def raw_tweets(parquet_files: Output) -> None:
    postgres_connection_string = os.getenv("POSTGRE_CONNECTION_STRING")
    table_name = "raw_tweets"
    schema_name = "public"
    if postgres_connection_string is None:
        raise Exception("POSTGRE_CONNECTION_STRING is not set")
    else:
        engine = create_engine(postgres_connection_string)

        try:
            with engine.connect() as conn:
                conn.execute(ddl["raw_tweets"])

                query = f"DELETE FROM {schema_name}.{table_name};"
                conn.execute(query)

            for df in parquet_files:
                _upload_data(engine, df, table_name, schema_name)
        finally:
            engine.dispose()
