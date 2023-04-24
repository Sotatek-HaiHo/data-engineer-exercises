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

ddl = {
    "raw_tweets": """
    create table if not exists raw_tweets
    (
        status_id            bigint,
        user_id              bigint,
        created_at           timestamp,
        screen_name          text,
        content                 text,
        source               text,
        reply_to_status_id   bigint,
        reply_to_user_id     bigint,
        reply_to_screen_name text,
        is_quote             boolean,
        is_retweet           boolean,
        favourites_count     integer,
        retweet_count        integer,
        country_code         text,
        place_full_name      text,
        place_type           text,
        followers_count      integer,
        friends_count        integer,
        account_lang         integer,
        account_created_at   timestamp,
        verified             boolean,
        lang                 text,
        tweet_date           date
    );
"""
}
