'''
User Interactions
 
You are a web developer working with various teams on your company's website. You have access to three separate DataFrames, each representing different types of user interaction with your website.
 
The first, page_visits, has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|   user_id   |  string   |
|   page_id   |  string   |
| visit_time  | timestamp |
+-------------+-----------+

 
The second, page_likes, has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|   user_id   |  string   |
|   page_id   |  string   |
|  like_time  | timestamp |
+-------------+-----------+

 
The third, page_comments, has the following schema:

+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
|   user_id    |  string   |
|   page_id    |  string   |
| comment_time | timestamp |
+--------------+-----------+

 
All three represent distinct user interactions - visits, likes, and comments. They all share the same columns: user_id, page_id, and a timestamp column, which represents the time of the interaction. However, the timestamp column has a different name in each DataFrame.
 
Write a function that combines these DataFrames and returns the following schema:

+------------------+-----------+
|   Column Name    | Data Type |
+------------------+-----------+
|     user_id      |  string   |
|     page_id      |  string   |
| interaction_time | timestamp |
| interaction_type |  string   |
+------------------+-----------+

 
The interaction_time column should contain the timestamp of the interaction and the interaction_type column should indicate the type of interaction - 'visit', 'like', or 'comment'.
 
 

Example

page_visits
+---------+---------+---------------------+
| user_id | page_id |     visit_time      |
+---------+---------+---------------------+
|   U1    |   P1    | 2023-01-01 12:00:00 |
|   U2    |   P3    | 2023-01-02 15:30:00 |
|   U3    |   P2    | 2023-01-03 10:45:00 |
+---------+---------+---------------------+

page_likes
+---------+---------+---------------------+
| user_id | page_id |      like_time      |
+---------+---------+---------------------+
|   U1    |   P2    | 2023-01-02 14:20:00 |
|   U2    |   P1    | 2023-01-03 16:40:00 |
|   U3    |   P3    | 2023-01-04 18:55:00 |
+---------+---------+---------------------+

page_comments
+---------+---------+---------------------+
| user_id | page_id |    comment_time     |
+---------+---------+---------------------+
|   U1    |   P3    | 2023-01-03 13:00:00 |
|   U2    |   P2    | 2023-01-04 17:10:00 |
|   U3    |   P1    | 2023-01-05 19:25:00 |
+---------+---------+---------------------+

Expected
+---------------------+------------------+---------+---------+
|  interaction_time   | interaction_type | page_id | user_id |
+---------------------+------------------+---------+---------+
| 2023-01-01 12:00:00 |      visit       |   P1    |   U1    |
| 2023-01-02 14:20:00 |       like       |   P2    |   U1    |
| 2023-01-02 15:30:00 |      visit       |   P3    |   U2    |
| 2023-01-03 10:45:00 |      visit       |   P2    |   U3    |
| 2023-01-03 13:00:00 |     comment      |   P3    |   U1    |
| 2023-01-03 16:40:00 |       like       |   P1    |   U2    |
| 2023-01-04 17:10:00 |     comment      |   P2    |   U2    |
| 2023-01-04 18:55:00 |       like       |   P3    |   U3    |
| 2023-01-05 19:25:00 |     comment      |   P1    |   U3    |
+---------------------+------------------+---------+---------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(page_visits, page_likes, page_comments):
	# Write code here

    # adding new column to identify table type to each df.
    page_visits = page_visits.withColumn("interaction_type",F.lit("visit"))
    page_likes = page_likes.withColumn("interaction_type",F.lit("like"))
    page_comments = page_comments.withColumn("interaction_type",F.lit("comment"))
    
    #rename timestamp column for each DF
    page_visits = page_visits.withColumnRenamed("visit_time","interaction_time")
    page_likes = page_likes.withColumnRenamed("like_time","interaction_time")
    page_comments = page_comments.withColumnRenamed("comment_time","interaction_time")
    
    final_df = page_visits\
                .select("interaction_time","interaction_type","page_id","user_id")\
                .unionAll(page_likes.select("interaction_time","interaction_type","page_id","user_id"))\
                .unionAll(page_comments.select("interaction_time","interaction_type","page_id","user_id"))
    return final_df
    
    
