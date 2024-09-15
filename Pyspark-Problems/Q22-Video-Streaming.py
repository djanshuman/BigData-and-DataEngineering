'''
Video Streaming Platform

 

A Streaming company wants to analyze its users' behavior and subscription data. They have the following DataFrames:


UserBehavior

 

+---------------+---------+
| Field         | Type    |
+---------------+---------+
| userId        | String  |
| watchDuration | Integer |
| date          | String  |
+---------------+---------+
- userId: A unique identifier for each user
- watchDuration: The total amount of minutes a user has watched content for on a particular date
- date: The date when the user watched the content (in the format YYYY-MM-DD)

Subscription

 

+-------------------+--------+
| Field             | Type   |
+-------------------+--------+
| userId            | String |
| subscriptionStart | String |
| subscriptionEnd   | String |
+-------------------+--------+
- userId: A unique identifier for each user
- subscriptionStart: The date when the user's subscription started (in the format YYYY-MM-DD)
- subscriptionEnd: The date when the user's subscription ended (in the format YYYY-MM-DD). If the user is still subscribed, this field will be "ongoing"

The company wants to know how many total minutes each user watched while they had an active subscription. For the purpose of this problem, assume a user's subscription is active on the day their subscription starts and ends.

 

Write a function that yields that accomplishes this and yields the schema below.

 

Note: If a user has multiple subscriptions in the data, include the watch time from all subscriptions. If a user watched content on a date when they didn't have an active subscription, do not include that watch time in the total.


Output Schema:

 

+----------------+---------+
| Field          | Type    |
+----------------+---------+
| userId         | String  |
| totalWatchTime | Integer |
+----------------+---------+
userId: A unique identifier for each user
totalWatchTime: The total watch time in minutes during an active subscription for the user
 


Example

 

UserBehavior
+--------+---------------+------------+
| userId | watchDuration | date       |
+--------+---------------+------------+
| U1     | 45            | 2023-01-01 |
| U1     | 60            | 2023-01-02 |
| U2     | 70            | 2023-01-03 |
| U3     | 30            | 2023-01-04 |
| U2     | 50            | 2023-01-02 |
+--------+---------------+------------+

Subscription
+--------+-------------------+-----------------+
| userId | subscriptionStart | subscriptionEnd |
+--------+-------------------+-----------------+
| U1     | 2023-01-01        | 2023-01-10      |
| U2     | 2023-01-02        | ongoing         |
| U3     | 2022-12-25        | 2023-01-05      |
+--------+-------------------+-----------------+

Expected
+----------------+--------+
| totalWatchTime | userId |
+----------------+--------+
| 105            | U1     |
| 120            | U2     |
| 30             | U3     |
+----------------+--------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(user_behavior_df, subscription_df):
	# Write code here

  # correct date format in UserBehavior DF

  user_behavior_df = user_behavior_df.withColumn(\
                        "date",
                        F.to_date(
                            F.col("date"),"yyyy-MM-dd"
                        ),
                        )
                       
  subscription_df = subscription_df.withColumn(\
                      "subscriptionStart",
                        F.to_date(
                            F.col("subscriptionStart"),"yyyy-MM-dd"),
                                )
                        
  subscription_df = subscription_df.withColumn(
                      "subscriptionEnd",
                        F.to_date(
                            F.col("subscriptionEnd"),"yyyy-MM-dd"),
                        )

  joinedDF =  user_behavior_df.join(subscription_df,on="userId",how="inner")
  filteredDF = joinedDF.filter(
                (
                    F.col("date") >= F.col("subscriptionStart")
                )
                 &
                (
                 (
                     F.col("date") <= F.col("subscriptionEnd")
                 ) 
                |
                 (F.col("subscriptionEnd").isNull()) 
                ))

  finalDF= filteredDF.groupBy("userId")\
                .agg(
                    F.sum("watchDuration").alias("totalWatchTime")
                    )
  return finalDF
