from pyspark.sql import SparkSession

# create SparkSession
spark = SparkSession.builder.appName("club_fact").enableHiveSupport().getOrCreate()

df = spark.sql("SELECT club, SUM(tweet_value) as total_tweet_value, count(1) as number_of_tweets, year, month, day, hour \
                FROM club_raw \
                GROUP BY club, year, month, day, hour \
               order by total_tweet_value desc,number_of_tweets desc")

df.write.partitionBy("year", "month", "day", "hour") \
           .saveAsTable("Club_processed", path="/twitter_processed_data", mode="overwrite")
