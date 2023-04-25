#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd
from dagster import AssetKey, build_input_context, build_output_context

from dagster_covid19.resources import DataFrameIOManager, DataFrameIOManagerConfig

from pandas.testing import assert_frame_equal


def should__save_and_load_dataframe_iterator():
    config = DataFrameIOManagerConfig.default()
    manager = DataFrameIOManager(config)
    df_1 = pd.DataFrame({"col1": [0, 1, 2, 3], "col2": [0, -1, -2, -3]})
    df_2 = pd.DataFrame({"col1": [4, 5, 6, 7], "col2": [-4, -5, -6, -7]})
    output_df = [df_1, df_2]
    output_context = build_output_context(
        name="result", asset_key=AssetKey(["test_asset"])
    )
    manager.handle_output(output_context, output_df)

    input_context = build_input_context(asset_key=AssetKey(["test_asset"]))
    input_df = manager.load_input(input_context)

    for df in output_df:
        assert_frame_equal(df, next(input_df))
