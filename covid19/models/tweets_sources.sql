-- tweets_sources.sql
-- Count tweets source of all user

{{
  config(
    materialized='table'
  )
}}

with src as (SELECT distinct user_id, tweet_date, coalesce(source, 'No Information') as source, COUNT(*) AS cnt
             FROM {{ source('raw_tweets', 'raw_tweets') }}
             GROUP BY source, user_id, tweet_date),
     sources as (SELECT distinct user_id, tweet_date, JSONB_OBJECT_AGG(src.source, cnt) AS tweet_sources
                 FROM src
                 group by user_id, tweet_date)
SELECT distinct user_id, tweet_date, tweet_sources
FROM sources
