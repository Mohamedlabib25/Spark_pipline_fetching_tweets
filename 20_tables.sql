drop table tweets;
CREATE EXTERNAL TABLE IF NOT EXISTS tweets(
  id STRING,
  text STRING,
  words ARRAY<STRING>,
  tweet_value DOUBLE,
  country_code STRING
  ) 
PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/twitter-landing-data';
MSCK REPAIR TABLE tweets;

drop table club_raw;
CREATE EXTERNAL TABLE IF NOT EXISTS club_raw (
  id STRING,
   club STRING,
     tweet_value DOUBLE
) 
PARTITIONED BY (year INT, month INT, day INT, hour INT)
LOCATION '/twitter-raw-data';

SET hive.exec.dynamic.partition.mode=nonstrict;  

INSERT   OVERWRITE TABLE  club_raw PARTITION(year, month, day, hour)
SELECT DISTINCT  id,
       CASE 
           WHEN LOWER(word) IN ("real", "madrid", "realmadrid", "halamadrid", "rmcf", "#halamadrid") THEN 'Real Madrid' 
           WHEN LOWER(word) IN ("manchester", "city", "manchestecity", "mcfc", "mancity", "#mancity") THEN 'Manchester City'
           WHEN LOWER(word) IN ("inter","intermilan" ,"internazionale", "#forzainter", "intermilano") THEN 'Inter Milan'
           WHEN LOWER(word) IN ("ac", "acmilan", "rossoneri", "#forzamilan", "acm") THEN 'AC Milan'
           
       END AS club ,tweet_value, year, month, day, hour
FROM tweets
LATERAL VIEW explode(words) wordsTable AS word
WHERE LOWER(word) IN ("real", "madrid", "realmadrid", "halamadrid", "rmcf", "#halamadrid", 
               "manchester", "city", "manchestecity", "mcfc", "mancity", "#mancity", 
               "inter", "intermilan", "internazionale", "#forzainter", "intermilano", 
               "ac", "acmilan", "rossoneri", "#forzamilan", "acm");
