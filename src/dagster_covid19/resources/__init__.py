#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Any, Mapping

from dagster_covid19.resources.dataframe_io_manager import (
    DataFrameIOManager,
    DataFrameIOManagerConfig,
)
from dagster_covid19.resources.dbt import get_dbt_cli
from dagster_covid19.resources.kaggle import get_kaggle_api
from dagster_covid19.resources.postgresql import get_postgresql_engine


def get_local_resources():
    local_resources = {
        "dbt": get_dbt_cli(),
        "kaggle_api": get_kaggle_api(),
        "postgresql": get_postgresql_engine(),
        "df_io_manager": DataFrameIOManager(DataFrameIOManagerConfig.default()),
    }
    return local_resources


def get_resources() -> Mapping[str, Any]:
    return get_local_resources()
