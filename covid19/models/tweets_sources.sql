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

with src as (SELECT distinct user_id, tweet_date, coalesce(source, 'No Information') as source, COUNT(*) AS cnt
             FROM {{ source('kaggle', 'covid19_tweets_table') }}
             GROUP BY source, user_id, tweet_date),
     sources as (SELECT distinct user_id, tweet_date, JSONB_OBJECT_AGG(src.source, cnt) AS tweet_sources
                 FROM src
                 group by user_id, tweet_date)
SELECT distinct user_id, tweet_date, tweet_sources
FROM sources
