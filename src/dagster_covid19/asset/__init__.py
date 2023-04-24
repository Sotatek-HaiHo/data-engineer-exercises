from dagster import Definitions
from src.dagster_covid19.asset.dbt import (
    dbt_assets,
    DBT_PROFILES,
    DBT_PROJECT_PATH,
    dbt_sources_sensor,
)

from dagster_covid19.asset.kaggle import kaggle_assets, kaggle_job

from dagster_covid19.resources import get_resources

defs = Definitions(
    assets=[*kaggle_assets, *dbt_assets],
    resources=get_resources(),
    sensors=[dbt_sources_sensor],
    jobs=[kaggle_job],
)
