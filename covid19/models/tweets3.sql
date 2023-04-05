-- tweets.sql

-- Load data from CSV files into a staging table
{{
  config(
    materialized='table'
  )
}}

SELECT DISTINCT user_id, old_screen_name, new_screen_name, change_date
FROM (SELECT user_id,
             screen_name                                                       AS old_screen_name,
             LEAD(screen_name) OVER (PARTITION BY user_id ORDER BY created_at) AS new_screen_name,
             LEAD(created_at) OVER (PARTITION BY user_id ORDER BY created_at)  AS change_date
      FROM public.raw_tweets) subquery
WHERE new_screen_name <> old_screen_name
