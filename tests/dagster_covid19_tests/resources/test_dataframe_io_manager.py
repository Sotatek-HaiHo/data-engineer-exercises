#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from dagster import AssetKey, build_input_context, build_output_context, build_resources

from dagster_covid19.config.datatypes import DataFrameIterator
from dagster_covid19.resources import dataframe_io_manager, tmp_dir

from pandas.testing import assert_frame_equal


def should__save_and_load_dataframe_iterator():
    with build_resources(
        resources={"tmp_dir": tmp_dir, "df_io_manager": dataframe_io_manager}
    ) as resources:
        manager = resources.df_io_manager
        df_1 = pd.DataFrame({"col1": [0, 1, 2, 3], "col2": [0, -1, -2, -3]})
        df_2 = pd.DataFrame({"col1": [4, 5, 6, 7], "col2": [-4, -5, -6, -7]})
        output_df = [df_1, df_2]
        output_context = build_output_context(
            name="result",
            asset_key=AssetKey(["test_asset"]),
            dagster_type=DataFrameIterator,
            run_id="run_1",
        )
        manager.handle_output(output_context, output_df)
        # Write twice to check for both writing and updating
        output_context_2 = build_output_context(
            name="result",
            asset_key=AssetKey(["test_asset"]),
            dagster_type=DataFrameIterator,
            run_id="run_2",
        )
        manager.handle_output(output_context_2, output_df)

        input_context = build_input_context(asset_key=AssetKey(["test_asset"]))
        input_df = manager.load_input(input_context)

        for df in output_df:
            assert_frame_equal(df, next(input_df))
