  # Spark_streaming_pipline_UCL_Clubs

This project involves building a pipeline to collect and process data from Twitter using Python, HDFS, Hive, and Spark. A Python script fetches tweets from the Twitter API, pushes them to a port, and stores them in HDFS. The data is then parsed, transformed, and partitioned. HiveQL queries extract dimensions, and SparkSQL is used for aggregations.

<div align="center">
  <img src="https://user-images.githubusercontent.com/132618266/236665004-cfe62e93-e5a8-42a9-a70e-b6cb74d8b651.png" alt="image" />
</div>


## Overview

This pipeline fetches tweets from Twitter using a Python script, `01_continous_listener.py`, which runs continuously and sends the data as JSON objects. It fetches tweets using a list of keywords which I chose to be the names of the 4 clubs in the Champions League and their most popular adjacent hashtags.

The goal is to find out which clubs have the largest measures for tweets (retweet_count, reply_count, like_count, quote_count, impression_count), as well as which club people talk about the most. In brief, this pipeline streams data from the Twitter API, sending it as JSON objects.

Here are some examples of the JSON objects that are fetched from the Twitter API:

 
 {"id": "1653911762872172547", "text": "RT @Joshua_Ubeku: What has Ronaldo won since he left his comfort zone at Real Madrid? Show me, please. I want to compare them with what Mes\\u2026", "retweet_count": 720, "reply_count": 0, "like_count": 0, "quote_count": 0, "impression_count": 0, "timestamp": "2023-05-03T23:57:47.000Z"}'
 
 {"id": "1653911897173901316", "text": "@rmfcron Real Madrid dominates the all-time UEFA Champions League rankings with AC Milan trailing behind. Want to shop like a champion? Check out TEMU\'s giveaway (link: https://t.co/oRujX26I5l) for a chance to win big! \\ud83d\\udecd\\ufe0f\\ud83c\\udfc6 https://t.co/KUHZjVvzK3", "retweet_count": 0, "reply_count": 0, "like_count": 0, "quote_count": 0, "impression_count": 6, "timestamp": "2023-05-03T23:58:19.000Z"}'
 
 
 
The final output is a sum of the tweets' measures and the count of tweets for each club from the four chosen ones, grouped by year, month, day, and hour.
 here is a sample from the fact table 
<div align="center">
  <img src="https://user-images.githubusercontent.com/132618266/236658007-d2ec98a8-a8a7-4a19-ba4d-0623986d9a96.png" alt="image" />
</div>



 
 # Pipeline Architecture Details

## Data Source System: `01_continuous_listener.py`

- The keywords used to measure metrics for tweets adjacent to four clubs are: `"real madrid"`, `"HalaMadrid"`, `"Manchester City"`, `"Cityzens"`, `"Inter MILAN"`, `"intermilan"`, `"ForzaInter"`, `"AC Milan"`, `"ForzaMilan"`.
- Modify the `start_time` and `end_time` to suit your current time.
- This listener is a continuous job that runs all the time with a final block of code in a `while true` loop.
- Data is sent as JSON objects to port 7777.
- Each JSON object has the following keys: `('id','text', 'retweet_count','retweet_count','reply_count','like_count','quote_count','impression_count','impression_count','timestamp')`





## Data Collection System : `01_structure_stream.py`

-	This part is mainly used as the data collector from the port, a link between Twitter API and a distributed storage system HDFS.
- I made another queue which called streaming and I sent this script to this queue so it will run companied with listener script so it will be like Long Running Job and it will keep receiving data from the port that was opened in the previous part of the pipeline.
- Inside it, i  received the Jason from 7777 port with a defined schema for the columns to  give it structure (Column Names and Data Types),
-  I made some transformations for the data so I extracted the year ,month ,day, hour from timestamp column .
- I assumed a function to replace the five measures for each tweet to replace them with only a column `tweet_value`.
- 	I cleaned the text from any mentions, popular emojis , urls by defining a remove_urls
- 	I defined a list ‘words’, which have  words (words related to each club) I need to search about them in the text column
-	Defining two udfs to search about any word in the list that can be in the text column and if that happens these words will be in the words column (array type).
-	It was the final step 01_structure_stream.py in the Here is the place where the data are stored persistently in its base format, just partitioned by (Year, Month, Day, Hour) in a directory called ` “twitter-landing-data” `, stored as `Parquet`.
- 	Also you will need to create a Hive Table  `“tweets” ` on top if this directory to be used later in the pipeline.
## Landing To Raw ETL

-	Here i run some HiveQL queries stored as tables.sql Scripts to extract the dimension from `tweets`.
-	
-	The output dimension is stored under a directory called `“/twitter-raw-data”` and 
-	I have in this pipeline only one dimension  `“club-raw”`  `(id STRING,   club STRING,     tweet_value DOUBLE) PARTITIONED BY (year INT, month INT, day INT, hour INT))`  , from my point of view I don’t any other dimensions for my goals in this project , I   tried to get info about the place but it seems that twitter API doesn’t support that.
-	Here I used `‘case when’` to replace the words array with the names of the club adjacent to this word and I used `explode` to convert the array to rows .

##  Raw To Processed ETL

-	Here, the aggregations to create the final `club_processed` is going to be created it is created by `grouping by club, year, month, day and hour `
-	I used a `SparkSQL` application for this step.
-	The fact table  contains `number_of_tweets` which is the count of the tweets at a specific point in time, sum of tweet_value  `total_tweet_value`
-	The output fact table should be stored under a directory called `“twitter-processed-data”` .
-	I used  `.saveAsTable`  to Store the output table should be created directly from the inside of the Spark App.







## Shell Script Coordinator

	I made two shell scripts :  `1_pipline.sh` , `21_tables.sh`  
 1. `1_pipline.sh`
    - `01_continous_listener.py`
    - `01_structure_stream.py` 
    - `&` for sending processing this command to the background so it can go  forward to the next line. 
    - `deploy-mode cluster ` for making this application to run in the cluster 
   	-  `queue streaming`  for sending this application to run in the streaming queue rather than the default queue 
 I made a new queue for the yarn service with the name streaming and with a capacity 30 %

2.	`21_tables.sh`  
    - `20_tables.sql`
    - `20_fact.py`
    - I used `crontab` to run the 21_tables.sh on the start of each hour like but it didnot work as expected 
       -  */5 * * * * /home/itversity/itversity-material/final/21_tables.sh


 
