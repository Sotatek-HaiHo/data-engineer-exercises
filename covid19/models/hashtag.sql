-- hashtag.sql
-- Get all hashtag in text field

{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['user_id'], 'type': 'hash'},
      {'columns': ['user_id', 'tweet_date'], 'unique': True}
    ]
  )
}}

with hst as (SELECT distinct user_id, tweet_date, unnest(regexp_matches(lower(content), '#\w+', 'g')) AS hashtag
             FROM {{ source('kaggle', 'covid19_tweets_table') }}),
     hst2 as (SELECT user_id, tweet_date, hashtag
              FROM hst)

select user_id,
       tweet_date,
       array_agg(hashtag) AS hashtags
from hst2
group by user_id, tweet_date
