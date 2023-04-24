import os
from typing import Iterable

import pandas as pd

from dagster import IOManager, OutputContext


class DataFrameIOManager(IOManager):
    filepath = os.getenv("DAGSTER_ASSET_TMPDIR")

    def _file_name(self, filepath: str, count: int, filename: str):
        return filepath + "/" + str(count) + filename

    def handle_output(
        self, context: OutputContext, obj: Iterable[pd.DataFrame]
    ) -> None:
        """
        Write an obj to a parquet file and save it in the directory.
        :param context: context of the output asset
        :param obj: obj to write to parquet file
        """
        filename = context.metadata.get("name")
        for count, element in enumerate(obj):
            parquet_file = self._file_name(self.filepath, count, filename)
            element.to_parquet(parquet_file)

    def load_input(self, context: OutputContext) -> Iterable[pd.DataFrame]:
        """
        Read parquet files and add the data they contain to the database table.
        :param context: context of the output asset
        :return: dataframe of the parquet files
        """
        filename = context.metadata.get("name")

        def parquet_df_gen(filename: str):
            for count, element in enumerate(os.listdir(self.filepath)):
                input_path = self._file_name(self.filepath, count, filename)
                par_df = pd.read_parquet(input_path)
                yield par_df

        parquet_generator = parquet_df_gen(filename)
        return parquet_generator
