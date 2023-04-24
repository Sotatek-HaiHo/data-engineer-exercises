from dagster import Definitions
from src.dagster_covid19.asset.asset_resources import resources

from src.dagster_covid19.asset.sensor import (
    dbt_assets,
    DBT_PROFILES,
    DBT_PROJECT_PATH,
    sensor_trigger_dbt_assets,
)

from dagster_covid19.asset.kaggle import kaggle_assets, kaggle_job

defs = Definitions(
    assets=[*kaggle_assets, *dbt_assets],
    resources=resources,
    sensors=[sensor_trigger_dbt_assets],
    jobs=[kaggle_job],
)
