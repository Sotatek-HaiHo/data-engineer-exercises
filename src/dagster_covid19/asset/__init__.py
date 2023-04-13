from dagster_dbt import load_assets_from_dbt_project
from dagster import file_relative_path
from dagster_dbt import dbt_cli_resource
from src.dagster_covid19 import asset
from src.dagster_covid19.asset.import_data import raw_tweets
from dagster import Definitions, load_assets_from_modules

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
a = load_assets_from_modules([asset])[:]
a.append(raw_tweets)
defs = Definitions(assets=a, resources=resources)