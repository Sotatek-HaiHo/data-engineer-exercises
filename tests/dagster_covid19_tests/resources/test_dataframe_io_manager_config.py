#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from pathlib import Path

from dagster_covid19.resources.dataframe_io_manager import DataFrameIOManagerConfig


def should__return_configured_path__when__using_constructor():
    test_path = "/tmp/df_1"
    test_config = DataFrameIOManagerConfig(base_path=test_path)
    assert test_config.base_path == Path(test_path)


def should__return_configured_path__when__using_env_var():
    test_path = "/tmp/df_2"
    os.environ["DAGSTER_DATAFRAME_IO_PATH"] = test_path
    test_config = DataFrameIOManagerConfig.default()
    assert test_config.base_path == Path(test_path)


def should__return_temporary_path__when__not_configured():
    test_path = "/tmp/df_3"
    if "DAGSTER_DATAFRAME_IO_PATH" in os.environ:
        del os.environ["DAGSTER_DATAFRAME_IO_PATH"]
    os.environ["DAGSTER_ASSET_TMPDIR"] = test_path
    test_config = DataFrameIOManagerConfig.default()
    assert test_config.base_path == Path(test_path) / "df_io_manager"
