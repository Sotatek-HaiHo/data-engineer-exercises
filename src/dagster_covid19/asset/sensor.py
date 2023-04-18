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

DBT_PROJECT_PATH = file_relative_path(__file__, "../../../covid19")
DBT_PROFILES = file_relative_path(__file__, "../../../.dbt")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["covid19"]
)

dbt_jobs = define_asset_job(name="all_dbt_assets", selection=dbt_assets)


@asset_sensor(
    name="my_sensor",
    asset_key=AssetKey(["raw_tweets", "raw_tweets"]),
    jobs=[dbt_jobs],
    minimum_interval_seconds=120,
)
def my_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    yield RunRequest(
        run_key=None,
        run_config={},
    )
