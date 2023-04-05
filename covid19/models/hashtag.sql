-- hashtag.sql
-- Get all hashtag in text field

{{
  config(
    materialized='table'
  )
}}

with ht as (SELECT user_id,
                   tweet_date,
                   unnest(regexp_matches(lower(text), '#\w+', 'g')) AS hashtag
            FROM public.raw_tweets)

select user_id,
       tweet_date,
       array_agg(ht.hashtag) AS hashtags
from ht
group by user_id, tweet_date

