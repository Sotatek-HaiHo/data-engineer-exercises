from dagster import Definitions, file_relative_path
from dagster_dbt import dbt_cli_resource, load_assets_from_dbt_project
from src.dagster_covid19 import asset
from src.dagster_covid19.asset.import_data import parquet_files, raw_tweets

DBT_PROJECT_PATH = file_relative_path(__file__, "../../../covid19")
DBT_PROFILES = file_relative_path(__file__, "../../../.dbt")

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH, profiles_dir=DBT_PROFILES, key_prefix=["covid19"]
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
defs = Definitions(assets=all_assets, resources=resources)
