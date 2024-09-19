'''
Amusement Park Outlier
 
You are a Data Analyst at an amusement park operator. You've been given two DataFrames. The first, rides, contains information about the rides in the amusement park, and the second, visitors, contains information about each visitor's ride history.
 
rides has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|   ride_id   |  string   |
|  ride_name  |  string   |
|    type     |  string   |
|  capacity   |  integer  |
+-------------+-----------+
 

visitors has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| visitor_id  |  string   |
|   ride_id   |  string   |
|  timestamp  | timestamp |
|   rating    |  integer  |
+-------------+-----------+
 

Write a function to identify the ride with the most anomalous average visitor rating. An anomalous ride is defined as a ride whose average rating is either significantly higher or lower than the average rating across all rides.
 
 
Output Schema:
+----------------+-----------+
|  Column Name   | Data Type |
+----------------+-----------+
|    ride_id     |  string   |
|   ride_name    |  string   |
| average_rating |   float   |
|  is_anomalous  |  boolean  |
+----------------+-----------+
 
 

Example

rides
+---------+----------------+-------------+----------+
| ride_id |   ride_name    |    type     | capacity |
+---------+----------------+-------------+----------+
|   r1    | Roller Coaster |   Thrill    |    24    |
|   r2    |  Ferris Wheel  | Observation |    60    |
|   r3    |   Log Flume    |    Water    |    16    |
|   r4    |  Bumper Cars   |   Family    |    20    |
|   r5    | Merry-Go-Round |   Classic   |    40    |
+---------+----------------+-------------+----------+

visitors
+------------+---------+---------------------+--------+
| visitor_id | ride_id |      timestamp      | rating |
+------------+---------+---------------------+--------+
|     v1     |   r1    | 2023-07-01 10:00:00 |   5    |
|     v2     |   r1    | 2023-07-01 10:30:00 |   4    |
|     v1     |   r2    | 2023-07-01 11:00:00 |   3    |
|     v3     |   r3    | 2023-07-01 11:30:00 |   2    |
|     v4     |   r4    | 2023-07-01 12:00:00 |   5    |
+------------+---------+---------------------+--------+

Expected
+----------------+----------+--------------+---------+----------------+-------------+
| average_rating | capacity | is_anomalous | ride_id |   ride_name    |    type     |
+----------------+----------+--------------+---------+----------------+-------------+
|      2.0       |    16    |      0       |   r3    |   Log Flume    |    Water    |
|      3.0       |    60    |      0       |   r2    |  Ferris Wheel  | Observation |
|      4.5       |    24    |      0       |   r1    | Roller Coaster |   Thrill    |
|      5.0       |    20    |      0       |   r4    |  Bumper Cars   |   Family    |
+----------------+----------+--------------+---------+----------------+-------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(rides, visitors):
	# Write code here

    # calculate avg rating per ride

    avg_rating_rides = visitors.groupBy("ride_id")\
                    .agg(
                F.avg("rating").cast("double").alias("average_rating")
                    )
    # overall avg rating for all rides
    # Calculate the overall average rating


    overall_avg_rating = avg_rating_rides.agg(
        F.avg("average_rating")
    ).collect()[0][0]

    #anomalies condition

    condition = ( \
            F.col("average_rating") > overall_avg_rating * 1.5 \
                )| \
            ( F.col("average_rating") < overall_avg_rating * 0.5 )
    

    anom_checkDF = avg_rating_rides.withColumn("is_anomalous",
                    condition)
    

    final_df = rides.join(anom_checkDF,"ride_id","inner").select(
            "average_rating",
            "capacity",
            "is_anomalous",
            "ride_id",
            "ride_name",
            "type"
    )
    return final_df
