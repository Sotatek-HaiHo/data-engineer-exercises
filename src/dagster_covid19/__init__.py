from dagster_dbt import dbt_cli_resource
from dagster_covid19 import asset
from dagster_covid19.asset import DBT_PROFILES, DBT_PROJECT_PATH
from dagster_covid19.import_data import raw_tweets
from dagster import Definitions, load_assets_from_modules

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
