import os
from typing import Any

import pandas as pd

from dagster import IOManager


class DataFrameIOManager(IOManager):
    def file_name(self, filepath: str, count: int, filename: str):
        return filepath + "/" + str(count) + filename

    def handle_output(self, context: Any, obj: Any):
        """
        Write an obj to a parquet file and save it in the directory.
        :param context: context of the output asset
        :param obj: obj to write to parquet file
        """
        output_path = os.getenv("DAGSTER_ASSET_TMPDIR")
        filename = context.metadata.get("name")
        for count, element in enumerate(obj):
            parquet_file = self.file_name(output_path, count, filename)
            element.to_parquet(parquet_file)

    def load_input(self, context: Any):
        """
        Read parquet files and add the data they contain to the database table.
        :param context: context of the output asset
        :return: dataframe of the parquet files
        """
        filename = context.metadata.get("name")

        def parquet_df_gen(filename: str):
            tmpdir = os.getenv("DAGSTER_ASSET_TMPDIR")
            for count, element in enumerate(os.listdir(tmpdir)):
                input_path = self.file_name(tmpdir, count, filename)
                par_df = pd.read_parquet(input_path)
                yield par_df

        parquet_generator = parquet_df_gen(filename)
        return parquet_generator
