-- tweets.sql

-- Load data from CSV files into a staging table
{{
  config(
    materialized='table'
  )
}}

SELECT t1.reply_date,
       t1.reply_user_id,
       t1.original_user_id,
       t1.reply_delay,
       t2.tweet_number
FROM {{ ref('reply_delay') }} AS t1
         LEFT JOIN
     {{ ref('tweet_number') }} AS t2
ON t1.reply_date = t2.reply_date AND t1.original_user_id = t2.original_user_id AND t1.reply_user_id = t2.reply_user_id
WHERE t1.is_quote = TRUE
