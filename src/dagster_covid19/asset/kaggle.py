#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import zipfile
from collections.abc import Iterator
from datetime import datetime
from pathlib import PurePath

import pandas as pd
from dagster import (
    asset,
    AssetIn,
    Bool,
    define_asset_job,
    Field,
    Nothing,
    OpExecutionContext,
    Output,
)
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine

from dagster_covid19.config.path import get_tmp_dir

kg = KaggleApi()
kg.authenticate()


@asset(
    config_schema={"force_download": Field(Bool, default_value=False)},
    key_prefix=["kaggle"],
)
def covid19_tweets_zip(context: OpExecutionContext) -> PurePath:
    dataset_name = "smid80/coronavirus-covid19-tweets-early-april"
    kaggle_ds_base_path = PurePath(
        os.getenv("DAGSTER_KAGGLE_DS_PATH", get_tmp_dir() / "kaggle_ds_path")
    )
    force_download = context.op_config["force_download"]
    output_path = kaggle_ds_base_path / "coronavirus-covid19-tweets-early-april.zip"
    context.log.info(
        "Start downloading Kaggle dataset %s to %s", dataset_name, output_path
    )
    kg.dataset_download_files(
        dataset=dataset_name,
        path=kaggle_ds_base_path,
        force=force_download,
        unzip=False,
    )
    context.log.info("Finish downloading Kaggle dataset %s", dataset_name)
    return output_path


@asset(key_prefix=["kaggle"])
def covid19_tweets_csv(
    context: OpExecutionContext, covid19_tweets_zip: PurePath
) -> PurePath:
    context.log.info("Start unpacking Kaggle dataset %s", covid19_tweets_zip)
    output_path = covid19_tweets_zip.parent / covid19_tweets_zip.stem
    with zipfile.ZipFile(covid19_tweets_zip) as z:
        z.extractall(output_path)
    context.log.info("Finish unpacking Kaggle dataset %s", covid19_tweets_zip)
    return output_path


@asset(
    io_manager_key="df_io_manager",
    key_prefix=["kaggle"],
    metadata={"name": "covid.parquet"},
)
def covid19_tweets_dataframe(
    context: OpExecutionContext, covid19_tweets_csv: PurePath
) -> Output[Iterator[pd.DataFrame]]:
    def df_gen():
        for filename in os.listdir(covid19_tweets_csv):
            context.log.info("Processing file %s", filename)
            if filename.endswith(".CSV"):
                date_str = filename.split(" ")[0]
                date = datetime.strptime(date_str, "%Y-%m-%d").date()
                df = pd.read_csv(os.path.join(covid19_tweets_csv, filename))
                df["tweet_date"] = date
                df.rename({"text": "content"}, axis=1, inplace=True)
                yield df

    df_generator = df_gen()
    return Output(df_generator)


@asset(
    key_prefix=["kaggle"],
    ins={
        "covid19_tweets_dataframe": AssetIn(
            metadata={"name": "covid.parquet"},
        )
    },
)
def covid19_tweets_table(covid19_tweets_dataframe) -> Nothing:
    raw_tweets_ddl = """
        create table raw_tweets
        (
            status_id            bigint,
            user_id              bigint,
            created_at           timestamp,
            screen_name          text,
            content              text,
            source               text,
            reply_to_status_id   bigint,
            reply_to_user_id     bigint,
            reply_to_screen_name text,
            is_quote             boolean,
            is_retweet           boolean,
            favourites_count     integer,
            retweet_count        integer,
            country_code         text,
            place_full_name      text,
            place_type           text,
            followers_count      integer,
            friends_count        integer,
            account_lang         integer,
            account_created_at   timestamp,
            verified             boolean,
            lang                 text,
            tweet_date           date
        );
    """
    postgres_connection_string = os.getenv("POSTGRE_CONNECTION_STRING")
    table_name = "raw_tweets"
    schema_name = "public"
    if postgres_connection_string is None:
        raise Exception("POSTGRE_CONNECTION_STRING is not set")
    else:
        engine = create_engine(postgres_connection_string)

        try:
            with engine.connect() as conn:
                # Drop old data
                query = f"DROP TABLE {schema_name}.{table_name};"
                conn.execute(query)
                # Re-create table structure
                conn.execute(raw_tweets_ddl)

            for df in covid19_tweets_dataframe:
                df.to_sql(
                    name=table_name,
                    schema=schema_name,
                    con=engine,
                    if_exists="append",
                    index=False,
                )
        finally:
            engine.dispose()


kaggle_assets = [
    covid19_tweets_zip,
    covid19_tweets_csv,
    covid19_tweets_dataframe,
    covid19_tweets_table,
]

kaggle_job = define_asset_job(name="00_kaggle_covid19_tweet", selection=kaggle_assets)
