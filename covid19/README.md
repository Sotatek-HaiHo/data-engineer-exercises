# Analyse Covid19 Data

## Description

Consider the dataset provided here:

<https://www.kaggle.com/datasets/smid80/coronavirus-covid19-tweets-early-april>

1. Create a derived dataset with the following information.

    * tweet_date: the date partition from the file name
    * user_id: the user_id from the source dataset
    * num_tweets: the number of tweets from the given user on this day
    * hashtags: an array of hashtags found in all the text fields for that user on that day.
    * For example, if I tweeted “Hello #world” and then “I’m a #data engineer” on one day,
    this field should look like: [“#world”, “#data”]
    * tweet_sources: for this user-day combination, a map of which source program they used, and the
    number of tweets from that source. For example, if I sent two messages with my Android phone, and
    three using Tweetdeck, this field should look like: {“Twitter for Android”: 2, “Tweetdeck”: 3}
    * screen_name: the screen_name associated with this user_id. If the user changed their screen name
    throughout the day, then display the screen_name associated with their final tweet of the day.

2. Create another derived dataset with the following information.
For each tweet where is_quote = TRUE, provide:

    * reply_date: the date partition from the file name of the reply tweet
    * reply_user_id: the user_id of the replier original_user_id: the user_id of the original user
    * reply_delay: if the original user’s tweet exists in the dataset,
    calculate the difference between the
    * created_at of the original user’s tweet and that of the replier’s tweet
    * tweet_number: for the user who replied, the tweet number of this reply in chronological order.
    For example, if I sent four tweets today (not necessarily replies themselves) before this reply tweet,
    this would be tweet_number = 5.

3. Create a derived dataset with the following information.
Consider whether you would use the source dataset to create this,
or the #1 derived dataset and explain why.

    * user_id: the user_id from the source dataset
    * old_screen_name: the previous screen name of the user
    * new_screen_name: the new screen name of the user
    * change_date: the date the user changed their screen name

## Instruction setup

### Create project dbt

1. Run command ```dbt init covid19``` to create dbt project covid19
2. Config profiles.yml to connect with Postgresql database

### Import data from dataset

1. Download dataset from <https://www.kaggle.com/datasets/smid80/coronavirus-covid19-tweets-early-april>
2. Extract dataset to an empty folder
3. Create raw table in the database with all columns of the dataset
4. Config params of upload_data.py (Connection to database, csv folder path, table name)
5. Run file upload_data.py to upload data to database
