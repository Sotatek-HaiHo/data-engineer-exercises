-- tweets1.sql

{{
  config(
    materialized='table'
  )
}}

SELECT r.tweet_date,
       r.user_id,
       count(r.status_id)             as num_tweets,
       h.hashtags,
       t.tweet_sources,
       r.screen_name
FROM {{ source('raw_tweets', 'raw_tweets') }} AS r
         left join {{ ref('tweets_sources') }} AS t on r.user_id = t.user_id and r.tweet_date = t.tweet_date
         left join {{ ref('hashtag') }} AS h on r.user_id = h.user_id and r.tweet_date = h.tweet_date
group by r.tweet_date, r.user_id, r.screen_name, h.hashtags, t.tweet_sources
