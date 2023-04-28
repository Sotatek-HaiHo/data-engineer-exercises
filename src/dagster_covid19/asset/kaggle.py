#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import zipfile
from pathlib import Path

import pandas as pd
from dagster import (
    asset,
    Bool,
    define_asset_job,
    DynamicPartitionsDefinition,
    Field,
    load_assets_from_current_module,
    OpExecutionContext,
)

from dagster_covid19.config.datatypes import DataFrameIterator
from dagster_covid19.config.path import get_tmp_dir
from dagster_covid19.config.sql_table import SqlTable

csv_partition = DynamicPartitionsDefinition(name="covid19_tweets_csv_output")


@asset(
    config_schema={"force_download": Field(Bool, default_value=False)},
    required_resource_keys={"kaggle_api"},
)
def covid19_tweets_zip(context: OpExecutionContext) -> Path:
    dataset_name = "smid80/coronavirus-covid19-tweets-early-april"
    kaggle_ds_base_path = Path(
        os.getenv("DAGSTER_KAGGLE_DS_PATH", get_tmp_dir() / "kaggle_ds_path")
    )
    force_download = context.op_config["force_download"]
    output_path = kaggle_ds_base_path / "coronavirus-covid19-tweets-early-april.zip"
    context.log.info(
        "Start downloading Kaggle dataset %s to %s", dataset_name, output_path
    )
    context.resources.kaggle_api.dataset_download_files(
        dataset=dataset_name,
        path=kaggle_ds_base_path,
        force=force_download,
        unzip=False,
    )
    context.log.info("Finish downloading Kaggle dataset %s", dataset_name)
    return output_path


@asset
def covid19_tweets_csv(
    context: OpExecutionContext, covid19_tweets_zip: Path
) -> dict[str, Path]:
    context.log.info("Start unpacking Kaggle dataset %s", covid19_tweets_zip)
    output_path = covid19_tweets_zip.parent / covid19_tweets_zip.stem
    with zipfile.ZipFile(covid19_tweets_zip) as z:
        z.extractall(output_path)
    context.log.info("Finish unpacking Kaggle dataset %s", covid19_tweets_zip)
    file_name_pattern = re.compile(r"([0-9\-]+) Coronavirus Tweets.CSV")
    result = {}
    partitions = []
    for file_name in os.listdir(output_path):
        search_result = re.search(file_name_pattern, file_name)
        date_str = search_result.group(1)
        file_path = output_path / file_name
        result[date_str] = file_path
        partitions.append(date_str)
    context.instance.add_dynamic_partitions(csv_partition.name, partitions)
    return result


@asset(io_manager_key="df_io_manager", partitions_def=csv_partition)
def covid19_tweets_dataframe(
    context: OpExecutionContext, covid19_tweets_csv: dict[str, Path]
) -> pd.DataFrame:
    partition_key = context.asset_partition_key_for_output()
    file_path = covid19_tweets_csv[partition_key]
    df = pd.read_csv(file_path)
    df["tweet_date"] = partition_key
    df.rename({"text": "content"}, axis=1, inplace=True)
    return df


@asset(
    required_resource_keys={"postgresql"},
)
def covid19_tweets_table_ddl(context: OpExecutionContext) -> tuple[str, str]:
    table_name = "raw_tweets"
    schema_name = "public"
    raw_tweets_ddl = f"""
            create table if not exists {schema_name}.{table_name}
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
            )
            PARTITION BY LIST(tweet_date);
        """
    with context.resources.postgresql.connect() as conn:
        # Drop old table data for migration
        query = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
        conn.execute(query)
        # Create table
        conn.execute(raw_tweets_ddl)
        return schema_name, table_name


@asset(io_manager_key="sql_table_io_manager", partitions_def=csv_partition)
def covid19_tweets_table(
    context: OpExecutionContext,
    covid19_tweets_table_ddl: tuple[str, str],
    covid19_tweets_dataframe: DataFrameIterator,
) -> SqlTable:
    schema_name = covid19_tweets_table_ddl[0]
    table_name_prefix = covid19_tweets_table_ddl[1]
    partition_name_safe = context.partition_key.replace("-", "_")
    partition_table_name = f"{table_name_prefix}_{partition_name_safe}"
    partition_create_ddl = f"""
    CREATE TABLE {partition_table_name}
    PARTITION OF {table_name_prefix}
    FOR VALUES IN ('{context.partition_key}')
    """
    partition_drop_ddl = f"DROP TABLE IF EXISTS {schema_name}.{partition_table_name}"
    return SqlTable(
        schema_name=schema_name,
        table_name=partition_table_name,
        create_ddl=partition_create_ddl,
        drop_ddl=partition_drop_ddl,
        content=covid19_tweets_dataframe,
    )


kaggle_assets = load_assets_from_current_module(
    group_name="kaggle", key_prefix=["kaggle"]
)

kaggle_job = define_asset_job(name="00_kaggle_covid19_tweet", selection=kaggle_assets)
