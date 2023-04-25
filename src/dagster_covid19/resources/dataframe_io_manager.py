#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from collections.abc import Iterator
from pathlib import Path

import pandas as pd

from dagster import AssetKey, InputContext, IOManager, OutputContext
from pydantic import BaseModel

from dagster_covid19.config.path import get_tmp_dir


class DataFrameIOManagerConfig(BaseModel):
    base_path: Path

    @staticmethod
    def default() -> "DataFrameIOManagerConfig":
        df_path = Path(
            os.getenv("DAGSTER_DATAFRAME_IO_PATH", get_tmp_dir() / "df_io_manager")
        )
        return DataFrameIOManagerConfig(base_path=df_path)


class DataFrameIOManager(IOManager):
    def __init__(self, config: DataFrameIOManagerConfig):
        self._base_path = config.base_path

    def _asset_path(self, asset_key: AssetKey) -> Path:
        return self._base_path / asset_key.to_python_identifier()

    @staticmethod
    def _file_name(count: int, asset_path: Path) -> Path:
        return asset_path / f"{count}.parquet"

    def handle_output(
        self, context: OutputContext, obj: Iterator[pd.DataFrame]
    ) -> None:
        """
        Write an obj to a parquet file and save it in the directory.
        :param context: context of the output asset
        :param obj: obj to write to parquet file
        """
        asset_path = self._asset_path(context.asset_key)
        context.log.info("Saving output of %s to %s", context.asset_key, asset_path)
        # Ensure asset_path exists before writing
        if not asset_path.exists():
            asset_path.mkdir(parents=True)
        for count, element in enumerate(obj):
            parquet_file = self._file_name(count, asset_path)
            element.to_parquet(parquet_file)

    def load_input(self, context: InputContext) -> Iterator[pd.DataFrame]:
        """
        Read parquet files and add the data they contain to the database table.
        :param context: context of the output asset
        :return: dataframe of the parquet files
        """
        asset_path = self._asset_path(context.asset_key)
        context.log.info("Loading input %s from %s", context.asset_key, asset_path)

        def parquet_df_gen():
            for count, element in enumerate(os.listdir(asset_path)):
                input_path = self._file_name(count, asset_path)
                par_df = pd.read_parquet(input_path)
                yield par_df

        parquet_generator = parquet_df_gen()
        return parquet_generator
