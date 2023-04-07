-- hashtag.sql
-- Get all hashtag in text field

{{
  config(
    materialized='table'
  )
}}

with hst as (SELECT distinct user_id, tweet_date, unnest(regexp_matches(lower(content), '#\w+', 'g')) AS hashtag
             FROM {{ source('raw_tweets', 'raw_tweets') }}),
     hst2 as (SELECT user_id, tweet_date, hashtag
              FROM hst)

select user_id,
       tweet_date,
       array_agg(hashtag) AS hastags
from hst2
group by user_id, tweet_date
