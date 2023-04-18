from dagster import Definitions
from dagster_dbt import dbt_cli_resource

from src.dagster_covid19.asset.import_data import parquet_files, raw_tweets
from src.dagster_covid19.asset.sensor import (
    dbt_assets,
    DBT_PROFILES,
    DBT_PROJECT_PATH,
    my_sensor,
)

resources = {
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    )
}

all_assets = []
all_assets.extend(dbt_assets)
all_assets.append(raw_tweets)
all_assets.append(parquet_files)
defs = Definitions(assets=all_assets, resources=resources, sensors=[my_sensor])
