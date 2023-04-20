from dagster_dbt import dbt_cli_resource
from src.dagster_covid19.asset.io_manager import DataFrameIOManager
from src.dagster_covid19.asset.sensor import DBT_PROFILES, DBT_PROJECT_PATH


resources = {
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),
    "df_io_manager": DataFrameIOManager(),
}
