{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['reply_user_id'], 'type': 'hash'},
      {'columns': ['reply_user_id', 'original_user_id', 'reply_date', 'tweet_number'], 'unique': True}
    ]
  )
}}

SELECT t1.tweet_date                                                      AS reply_date,
       t1.user_id                                                         AS reply_user_id,
       t2.user_id                                                         AS original_user_id,
       ROW_NUMBER() OVER (PARTITION BY t1.user_id ORDER BY t1.created_at) AS tweet_number
FROM {{ source('kaggle', 'covid19_tweets_table') }} t1
         LEFT JOIN
     {{ source('kaggle', 'covid19_tweets_table') }} t2
ON t1.reply_to_status_id = t2.status_id
WHERE t1.is_quote = TRUE
