-- tweets_sources.sql
-- Count tweets source of all user

{{
  config(
    materialized='table'
  )
}}

WITH lscr_name as (SELECT DISTINCT ON (user_id, tweet_date) user_id, tweet_date, screen_name
                   FROM {{ source('raw_tweets', 'raw_tweets') }}
                   ORDER BY user_id, tweet_date, created_at DESC)
select distinct user_id, tweet_date, screen_name
from lscr_name
