#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from dagster import (
    asset_sensor,
    AssetKey,
    define_asset_job,
    EventLogEntry,
    RunRequest,
    SensorEvaluationContext,
)
from dagster_dbt import load_assets_from_dbt_project

from dagster_covid19.resources.dbt import DBT_PROFILES, DBT_PROJECT_PATH

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
