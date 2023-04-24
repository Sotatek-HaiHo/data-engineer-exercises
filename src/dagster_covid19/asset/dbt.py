#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from dagster import (
    asset_sensor,
    AssetKey,
    define_asset_job,
    EventLogEntry,
    file_relative_path,
    RunRequest,
    SensorEvaluationContext,
)
from dagster_dbt import load_assets_from_dbt_project

DEFAULT_PROJECT_PATH = file_relative_path(__file__, "../../../covid19")
DEFAULT_PROFILES_PATH = file_relative_path(__file__, "../../../.dbt")

DBT_PROJECT_PATH = os.getenv("DBT_PROJECT_PATH", DEFAULT_PROJECT_PATH)
DBT_PROFILES = os.getenv("DBT_PROFILES", DEFAULT_PROFILES_PATH)

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["covid19"]
)

dbt_jobs = define_asset_job(name="all_dbt_assets", selection=dbt_assets)


@asset_sensor(
    asset_key=AssetKey(["kaggle", "covid19_tweets_table"]),
    jobs=[dbt_jobs],
)
def dbt_sources_sensor(
    context: SensorEvaluationContext, asset_event: EventLogEntry
) -> RunRequest:
    yield RunRequest()
