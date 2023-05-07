# Spark_streaming_pipline_to_processed_data
 the project involves building a pipeline to collect and process data from Twitter using Python, (HDFS), Hive, and Spark. A Python script fetches tweets from the Twitter API, pushes them to a port, and stores them in HDFS. The data is then parsed, transformed, and partitioned. HiveQL queries extract dimensions and SparkSQL is used for aggregations
 This pipline is about fetching tweets from twitter using a python script  01_continous_listener.py , it runs all the time and sends the data into a port . it fetches twitter using  a list of keywords which i choose to be the names of the 4 clubs in the champions league 
 and their adjacent most famous hashtags, i want to know which clubs has the largest measures for tweets ( retweet_count,reply_count,like_count,quote_count,impression_count) also i wanted which is the most club people talk about it.
 in brief i wanted to stream data from twitter API which and sending it like here is some  examples for some jason objects i asked twitter API for it 
 
 {"id": "1653911762872172547", "text": "RT @Joshua_Ubeku: What has Ronaldo won since he left his comfort zone at Real Madrid? Show me, please. I want to compare them with what Mes\\u2026", "retweet_count": 720, "reply_count": 0, "like_count": 0, "quote_count": 0, "impression_count": 0, "timestamp": "2023-05-03T23:57:47.000Z"}'
 {"id": "1653911897173901316", "text": "@rmfcron Real Madrid dominates the all-time UEFA Champions League rankings with AC Milan trailing behind. Want to shop like a champion? Check out TEMU\'s giveaway (link: https://t.co/oRujX26I5l) for a chance to win big! \\ud83d\\udecd\\ufe0f\\ud83c\\udfc6 https://t.co/KUHZjVvzK3", "retweet_count": 0, "reply_count": 0, "like_count": 0, "quote_count": 0, "impression_count": 6, "timestamp": "2023-05-03T23:58:19.000Z"}'
 
  and my final output would be like 
  measuring the tweets' measures and the count of tweets for each of the 4 clubs with also grouping by the year, month, day , hour 
  
 hive> select * from **club_processed**;
Real Madrid     27894.6         39      2023    4       30      23
Manchester City 11467.6         14      2023    4       30      23
AC Milan        8411.6          13      2023    4       30      23
Inter Milan     2998.0          5       2023    4       30      23
Real Madrid     1021564.4       796     2023    5       5       23
AC Milan        198357.0        224     2023    5       5       23
Manchester City 94100.6         109     2023    5       5       23
Inter Milan     67602.0         88      2023    5       5       23
 
 here is the schema for thee **fact table**

hive> describe club_processed;
OK
club                    string                                      
total_tweets_value       double                                      
number_of_tweets        bigint                                      
year                    int                                         
month                   int                                         
day                     int                                         
hour                    int                                         
# Partition Information          
# col_name              data_type               comment             
year                    int                                         
month                   int                                         
day                     int                                         
hour                    int                        
 
 
 
