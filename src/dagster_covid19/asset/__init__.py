from dagster import Definitions
from src.dagster_covid19.asset.asset_resources import resources

from src.dagster_covid19.asset.import_data import parquet_files, raw_tweets
from src.dagster_covid19.asset.sensor import (
    dbt_assets,
    DBT_PROFILES,
    DBT_PROJECT_PATH,
    sensor_trigger_dbt_assets,
)

all_assets = []
all_assets.extend(dbt_assets)
all_assets.append(raw_tweets)
all_assets.append(parquet_files)
defs = Definitions(
    assets=all_assets, resources=resources, sensors=[sensor_trigger_dbt_assets]
)
