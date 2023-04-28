#!/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Union

from dagster import (
    AssetKey,
    define_asset_job,
    multi_asset_sensor,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    SkipReason,
)
from dagster_dbt import load_assets_from_dbt_project

from dagster_covid19.resources.dbt import DBT_PROFILES, DBT_PROJECT_PATH

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROFILES,
    key_prefix=["covid19"],
    node_info_to_group_fn=lambda _: "dbt",
)

dbt_jobs = define_asset_job(name="all_dbt_assets", selection=dbt_assets)


@multi_asset_sensor(
    monitored_assets=[AssetKey(["kaggle", "covid19_tweets_table"])], jobs=[dbt_jobs]
)
def dbt_partitioned_sources_sensor(
    context: MultiAssetSensorEvaluationContext,
) -> Union[RunRequest, SkipReason]:
    asset_key = AssetKey(["kaggle", "covid19_tweets_table"])
    if context.all_partitions_materialized(asset_key):
        context.advance_all_cursors()
        return RunRequest(run_key=context.cursor)
    else:
        return SkipReason("Not enough materialized partitions")


dbt_sensors = [dbt_partitioned_sources_sensor]
