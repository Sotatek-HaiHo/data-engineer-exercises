#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
from dagster import InputContext, OutputContext, UPathIOManager
from pydantic import BaseModel
from upath import UPath

from dagster_covid19.config.path import get_tmp_dir


class DataFrameIOManagerConfig(BaseModel):
    base_path: Path

    @staticmethod
    def default() -> "DataFrameIOManagerConfig":
        df_path = Path(
            os.getenv("DAGSTER_DATAFRAME_IO_PATH", get_tmp_dir() / "df_io_manager")
        )
        return DataFrameIOManagerConfig(base_path=df_path)


class DataFrameIOManager(UPathIOManager):
    def __init__(self, config: DataFrameIOManagerConfig):
        super().__init__(UPath(config.base_path))

    @staticmethod
    def _file_name(count: int, asset_path: UPath) -> UPath:
        return asset_path / f"{count}.parquet"

    def dump_to_path(
        self, context: OutputContext, obj: Iterator[pd.DataFrame], path: UPath
    ):
        """
        Write an obj to a parquet file and save it in the directory.
        :param context: context of the output asset
        :param obj: obj to write to parquet file
        :param path: directory to write to
        """
        context.log.info("Saving output of %s to %s", context.asset_key, path)
        for count, element in enumerate(obj):
            parquet_file = self._file_name(count, path)
            element.to_parquet(parquet_file)

    def load_from_path(
        self, context: InputContext, path: UPath
    ) -> Iterator[pd.DataFrame]:
        """
        Read parquet files and add the data they contain to the database table.
        :param context: context of the output asset
        :param path: directory to read from
        :return: dataframe of the parquet files
        """
        context.log.info("Loading input %s from %s", context.asset_key, path)

        def parquet_df_gen():
            for count, element in enumerate(os.listdir(path)):
                input_path = self._file_name(count, path)
                par_df = pd.read_parquet(input_path)
                yield par_df

        parquet_generator = parquet_df_gen()
        return parquet_generator
