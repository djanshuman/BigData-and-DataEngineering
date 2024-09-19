'''
User Interactions
 

You are given a DataFrame that represents user interactions on a popular social media platform. Each row represents a single interaction between two users.
 
The DataFrame schema is as follows:

+------------------+-----------+
|   Column Name    | Data Type |
+------------------+-----------+
|  interaction_id  |  integer  |
|     user1_id     |  integer  |
|     user2_id     |  integer  |
| interaction_type |  string   |
|    timestamp     | timestamp |
+------------------+-----------+

interaction_id is the unique identifier for the interaction.
user1_id is the identifier of the first user involved in the interaction.
user2_id is the identifier of the second user involved in the interaction.
interaction_type is the type of interaction that occurred (e.g., 'like', 'comment', 'share').
timestamp is the time at which the interaction occurred.
 
Write a function that finds users who have interacted with themselves. This is possible when a user makes a post and then likes, comments, or shares it themselves. The output should include the user id and the count of self-interactions.
 
The output will have the following schema:

+------------------------+-----------+
|      Column Name       | Data Type |
+------------------------+-----------+
|        user_id         |  integer  |
| self_interaction_count |  integer  |
+------------------------+-----------+

user_id is the identifier of the user.
self_interaction_count is the number of times the user has interacted with their own posts.
 

Example

input_df
+----------------+----------+----------+------------------+---------------------+
| interaction_id | user1_id | user2_id | interaction_type |      timestamp      |
+----------------+----------+----------+------------------+---------------------+
|       1        |   1001   |   2002   |       like       | 2023-01-01 10:00:00 |
|       2        |   1002   |   1002   |     comment      | 2023-01-01 11:00:00 |
|       3        |   1003   |   2003   |      share       | 2023-01-02 10:00:00 |
|       4        |   1004   |   1004   |       like       | 2023-01-02 11:00:00 |
|       5        |   1005   |   2005   |     comment      | 2023-01-03 10:00:00 |
+----------------+----------+----------+------------------+---------------------+

Expected
+------------------------+----------+
| self_interaction_count | user1_id |
+------------------------+----------+
|           1            |   1002   |
|           1            |   1004   |
+------------------------+----------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here

    newDF = input_df.withColumn("self_interaction_count",
                               F.when(
                                F.col("user1_id") == F.col("user2_id"),1
                               ).otherwise(0))
    
    final_df = newDF.filter(F.col("self_interaction_count")==1)
    aggDF = final_df.groupBy("user1_id").\
        agg(
            F.sum("self_interaction_count").alias("self_interaction_count")
        )
    return aggDF
