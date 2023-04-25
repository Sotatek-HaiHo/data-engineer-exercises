#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import Iterable

import pandas as pd

from dagster import IOManager, OutputContext
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

    def _file_name(self, count: int, filename: str) -> Path:
        return self._base_path / f"{filename}_{count}.parquet"

    def handle_output(
        self, context: OutputContext, obj: Iterable[pd.DataFrame]
    ) -> None:
        """
        Write an obj to a parquet file and save it in the directory.
        :param context: context of the output asset
        :param obj: obj to write to parquet file
        """
        # Ensure base path exists before writing
        if not self._base_path.exists():
            self._base_path.mkdir(parents=True)
        filename = context.metadata.get("name")
        for count, element in enumerate(obj):
            parquet_file = self._file_name(count, filename)
            element.to_parquet(parquet_file)

    def load_input(self, context: OutputContext) -> Iterable[pd.DataFrame]:
        """
        Read parquet files and add the data they contain to the database table.
        :param context: context of the output asset
        :return: dataframe of the parquet files
        """
        filename = context.metadata.get("name")

        def parquet_df_gen(filename: str):
            for count, element in enumerate(os.listdir(self._base_path)):
                input_path = self._file_name(count, filename)
                par_df = pd.read_parquet(input_path)
                yield par_df

        parquet_generator = parquet_df_gen(filename)
        return parquet_generator
