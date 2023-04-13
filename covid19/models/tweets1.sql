-- tweets1.sql

{{
  config(
    materialized='table'
  )
}}

WITH nt as (
	SELECT tweet_date,user_id, count(status_id) as num_tweets
	from {{ source('raw_tweets', 'raw_tweets') }}
	group by tweet_date, user_id
)

SELECT r.tweet_date,
       r.user_id,
       r.num_tweets,
       h.hashtags,
       t.tweet_sources,
       l.screen_name
FROM nt AS r
         left join {{ ref('tweets_sources') }} AS t on r.user_id = t.user_id and r.tweet_date = t.tweet_date
         left join {{ ref('hashtag') }} AS h on r.user_id = h.user_id and r.tweet_date = h.tweet_date
         left join {{ ref('screen_name') }} l on l.user_id = r.user_id and l.tweet_date = r.tweet_date
