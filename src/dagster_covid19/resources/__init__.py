#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Any, Mapping

from dagster_dbt import dbt_cli_resource

from .dataframe_io_manager import DataFrameIOManager, DataFrameIOManagerConfig
from .dbt import DBT_PROFILES, DBT_PROJECT_PATH
from .kaggle import get_kaggle_api


def get_local_resources():
    local_resources = {
        "dbt": dbt_cli_resource.configured(
            {
                "project_dir": DBT_PROJECT_PATH,
                "profiles_dir": DBT_PROFILES,
            },
        ),
        "kaggle_api": get_kaggle_api(),
        "df_io_manager": DataFrameIOManager(DataFrameIOManagerConfig.default()),
    }
    return local_resources


def get_resources() -> Mapping[str, Any]:
    return get_local_resources()
