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
           WHEN LOWER(word) IN ("real madrid", "realmadrid", "halamadrid", "rmcf", "#halamadrid") THEN 'Real Madrid' 
           WHEN LOWER(word) IN ("Manchestercity","Manchester City","MCFC","#ManCity") THEN 'Manchester City'
           WHEN LOWER(word) IN ("intermilan","#ForzaInter","FCIM","Inter Milan") THEN 'Inter Milan'
           WHEN LOWER(word) IN ("acmilan", "rossoneri", "#forzamilan", "acm") THEN 'AC Milan'
           
       END AS club ,tweet_value, year, month, day, hour
FROM tweets
LATERAL VIEW explode(words) wordsTable AS word
WHERE LOWER(word) IN ( "real madrid", "realmadrid", "#halamadrid", "rmcf",
              "Manchestercity","Manchester City","MCFC","#ManCity",
               "intermilan","#ForzaInter","FCIM","Inter Milan" ,
               "AC Milan","Rossoneri","#ForzaMilan","ACM");
