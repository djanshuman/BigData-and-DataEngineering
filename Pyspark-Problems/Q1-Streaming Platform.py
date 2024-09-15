
/*

Streaming Platform

You work for a video streaming platform and are given a DataFrame containing information about videos available on the platform. The DataFrame named input_df has the following schema:



+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| video_id     | Integer   |
| title        | String    |
| genre        | String    |
| release_year | Integer   |
| duration     | Integer   |
| view_count   | Integer   |
+--------------+-----------+


Your task is to write a function that takes in the input DataFrame and returns a DataFrame containing only the videos with more than 1,000,000 views and released in the last 5 years. The output DataFrame should have the same schema as the input DataFrame.


Example



input_df
+----------+----------------------+--------+--------------+----------+------------+
| video_id | title                | genre  | release_year | duration | view_count |
+----------+----------------------+--------+--------------+----------+------------+
| 1        | Amazing Adventure    | Action | 2020         | 120      | 2500000    |
| 2        | Sci-fi World         | Sci-fi | 2018         | 140      | 800000     |
| 3        | Mysterious Island    | Drama  | 2022         | 115      | 1500000    |
| 4        | Uncharted Realms     | Action | 2019         | 134      | 3200000    |
| 5        | Journey to the Stars | Sci-fi | 2021         | 128      | 1100000    |
+----------+----------------------+--------+--------------+----------+------------+

Output
+----------+--------+--------------+----------------------+----------+------------+
| duration | genre  | release_year | title                | video_id | view_count |
+----------+--------+--------------+----------------------+----------+------------+
| 115      | Drama  | 2022         | Mysterious Island    | 3        | 1500000    |
| 120      | Action | 2020         | Amazing Adventure    | 1        | 2500000    |
| 128      | Sci-fi | 2021         | Journey to the Stars | 5        | 1100000    |
| 134      | Action | 2019         | Uncharted Realms     | 4        | 3200000    |
+----------+--------+--------------+----------------------+----------+------------+
*/
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
	current_year= datetime.datetime.now().year
  result_df= input_df.where( (F.col("view_count") > 1000000)
                             & (F.col("release_year") >= current_year - 5
                               )
                             )
return result_df
                    
    
