#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from dagster import file_relative_path, ResourceDefinition

from dagster_dbt import dbt_cli_resource

DEFAULT_PROJECT_PATH = file_relative_path(__file__, "../../../covid19")
DEFAULT_PROFILES_PATH = file_relative_path(__file__, "../../../.dbt")

DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH", DEFAULT_PROJECT_PATH)
DBT_PROFILES = os.getenv("DBT_PROFILES", DEFAULT_PROFILES_PATH)


def get_dbt_cli() -> ResourceDefinition:
    return dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    )
