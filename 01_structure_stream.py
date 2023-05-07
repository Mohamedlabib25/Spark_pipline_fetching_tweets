from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, explode
from pyspark.sql.functions import from_json, col,collect_list
#from pyspark.sql.functions import regexp_extract_all
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,DoubleType,ArrayType
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col
import re
spark = SparkSession.builder.appName("StructuredJsonStreaming").getOrCreate()

# Define the schema for your JSON data
schema = StructType([
    StructField("id", StringType()),
    StructField("text", StringType()),
    StructField("retweet_count", DoubleType()),
    StructField("reply_count", DoubleType()),
    StructField("like_count", DoubleType()),
    StructField("quote_count", DoubleType()),
    StructField("impression_count", DoubleType()),
    StructField("timestamp", TimestampType()),
    StructField("country_code", StringType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType()),
    StructField("hour", IntegerType())    
])

# Read the data from the socket
df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 7777)\
    .load() \
    .select(from_json(col("value"), schema).alias("data")) \
    .selectExpr(
        "data.id",
        "data.text",
        "data.retweet_count",
        "data.reply_count",
        "data.like_count",
        "data.quote_count",
        "data.impression_count",
        "data.timestamp",
        "data.country_code",
        "year(data.timestamp) as year",
        "month(data.timestamp) as month",
        "day(data.timestamp)  as day", 
        "hour(data.timestamp) as hour"
    ) 
    

#replacing all cols related to the tweet measures by a new col called "tweet_value" which i assume that its value will be calculated as the next function.
df = df.withColumn("tweet_value", (col("retweet_count") * 10 + col("reply_count") * 5 + col("like_count") * 10 + col("quote_count") * 5 + col("impression_count") ) / 5)

# Replace emojis with an empty string
df = df.withColumn("text", regexp_replace("text",u'[\U0001F601-\U0001F64D\U0001F64F\U0001F650-\U0001F67F\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U000024C2-\U0001F251\U0001F9D1\u200D\U0001F3A4]+', ' '))

# assuming your DataFrame is called "tweets_df" and the text column is called "text"
df = df.withColumn("text", regexp_replace("text", "@\\w+", ""))

# Remove any "\n" from the clean_text column
df = df.withColumn("text", regexp_replace("text", "\n", " "))

# Define a regular expression pattern to match URLs
url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

from pyspark.sql.functions import udf
# Define a UDF to remove URLs from text
def remove_urls(text):
    return url_pattern.sub('', text)

# # Register the UDF
remove_urls_udf = udf(remove_urls, StringType())

# # Apply the UDF to the 'text' column
df = df.withColumn('text', remove_urls_udf('text'))


from pyspark.sql.functions import trim

df = df.withColumn("text", trim(df["text"]))



# Define the list of words you want to filter in the text column
words = [ "madrid","realmadrid","halamadrid","RMCF", "Manchester","ManchesteCity","MCFC","ManCity","ManCity",
                  "intermilan","inter","ForzaInter","Internazionale","FCIM" ,"InterMilano","ac","AcMilan","Rossoneri","ForzaMilan","ACM"]

# Define a UDF to extract words from text
def extract_words(text):
    return [word for word in words if word.lower() in text.lower()]

# Register the UDF
extract_words_udf = udf(extract_words, ArrayType(StringType()))

# Apply the UDF to the 'text' column and create a new column called 'filtered_words'
df = df.withColumn('words', extract_words_udf('text'))



df = df.select("id","text","words","tweet_value","year","month","day","hour","country_code" )

# Write the streaming data to HDFS in parquet format
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/check")\
    .option("path","/twitter-landing-data")\
    .partitionBy("year", "month", "day", "hour") \
    .start()


query.awaitTermination()