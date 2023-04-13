{{
  config(
    materialized='table'
  )
}}
SELECT t1.tweet_date                                                             AS reply_date,
       t1.user_id                                                                AS reply_user_id,
       t2.user_id                                                                AS original_user_id,
       EXTRACT(EPOCH FROM (t1.created_at::TIMESTAMP - t2.created_at::TIMESTAMP)) AS reply_delay
FROM {{ source('raw_tweets', 'raw_tweets') }} t1
LEFT JOIN
{{ source('raw_tweets', 'raw_tweets') }} t2
ON t1.reply_to_status_id = t2.status_id
WHERE t1.is_quote = TRUE
