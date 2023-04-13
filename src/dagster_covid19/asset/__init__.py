from dagster_dbt import load_assets_from_dbt_project
from dagster import file_relative_path
from dagster_dbt import dbt_cli_resource
from src.dagster_covid19 import asset
from src.dagster_covid19.asset.import_data import raw_tweets
from dagster import Definitions

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
defs = Definitions(assets=all_assets, resources=resources)
