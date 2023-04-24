from dagster_dbt import dbt_cli_resource
from src.dagster_covid19.asset.dbt import DBT_PROFILES, DBT_PROJECT_PATH

from dagster_covid19.resources.dataframe_io_manager import (
    DataFrameIOManager,
    DataFrameIOManagerConfig,
)

resources = {
    "dbt": dbt_cli_resource.configured(
        {
            "project_dir": DBT_PROJECT_PATH,
            "profiles_dir": DBT_PROFILES,
        },
    ),
    "df_io_manager": DataFrameIOManager(DataFrameIOManagerConfig.default()),
}
