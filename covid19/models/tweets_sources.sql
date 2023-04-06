-- tweets_sources.sql
-- Count tweets source of all user

{{
  config(
    materialized='table'
  )
}}

with ts as (SELECT user_id,
                   tweet_date,
                   source,
                   COUNT(*) AS cnt
            FROM {{ source('raw_tweets', 'raw_tweets') }}
            GROUP BY source, user_id, tweet_date)

select ts.user_id,
       ts.tweet_date,
       JSONB_OBJECT_AGG(ts.source, ts.cnt) AS tweet_sources
from ts
where ts.source is not null
group by user_id, tweet_date
