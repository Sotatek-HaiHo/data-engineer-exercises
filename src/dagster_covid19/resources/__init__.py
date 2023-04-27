#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Any, Mapping

from dagster_covid19.resources.dataframe_io_manager import dataframe_io_manager
from dagster_covid19.resources.dbt import dbt_cli
from dagster_covid19.resources.kaggle import kaggle_api
from dagster_covid19.resources.path import tmp_dir
from dagster_covid19.resources.postgresql import postgresql_engine
from dagster_covid19.resources.table_io_manager import sql_table_io_manager


def get_local_resources():
    local_resources = {
        "tmp_dir": tmp_dir,
        "dbt": dbt_cli(),
        "kaggle_api": kaggle_api,
        "postgresql": postgresql_engine,
        "sql_table_io_manager": sql_table_io_manager,
        "df_io_manager": dataframe_io_manager,
    }
    return local_resources


def get_resources() -> Mapping[str, Any]:
    return get_local_resources()
