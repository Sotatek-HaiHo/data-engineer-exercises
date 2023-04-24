-- tweets_sources.sql
-- Count tweets source of all user

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['user_id'], 'type': 'hash'},
      {'columns': ['user_id', 'tweet_date'], 'unique': True}
    ]
  )
}}

WITH lscr_name as (SELECT DISTINCT ON (user_id, tweet_date) user_id, tweet_date, screen_name
                   FROM {{ source('kaggle', 'covid19_tweets_table') }}
                   ORDER BY user_id, tweet_date, created_at DESC)
select distinct user_id, tweet_date, screen_name
from lscr_name
