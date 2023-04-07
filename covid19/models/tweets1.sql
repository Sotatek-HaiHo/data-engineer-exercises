-- tweets1.sql

{{
  config(
    materialized='table'
  )
}}

SELECT r.tweet_date,
       r.user_id,
       count(r.status_id)             as num_tweets,
       array_agg(distinct h.hashtag) AS hashtags,
       t.tweet_sources,
       l.screen_name
FROM {{ source('raw_tweets', 'raw_tweets') }} AS r
         left join {{ ref('tweets_sources') }} AS t on r.user_id = t.user_id
         left join {{ ref('hashtag') }} AS h on r.user_id = h.user_id and r.tweet_date = h.tweet_date
         left join {{ ref('screen_name') }} l on l.user_id = r.user_id and l.tweet_date = r.tweet_date
group by r.tweet_date, r.user_id, r.screen_name, h.hashtag, t.tweet_sources, l.screen_name
