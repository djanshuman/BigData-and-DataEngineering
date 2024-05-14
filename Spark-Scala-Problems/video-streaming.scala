/* 
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
*/


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(user_behavior_df: DataFrame, subscription_df: DataFrame): DataFrame = {
    val user_behavior_df_converted = user_behavior_df
                            .withColumn("date",to_date(col("date"),"yyyy-MM-dd"))
    
    val subscription_df_converted =  subscription_df
        .withColumn("subscriptionStart",to_date(col("subscriptionStart"),"yyyy-MM-dd"))
        .withColumn("subscriptionEnd",to_date(col("subscriptionEnd"),"yyyy-MM-dd"))
        .withColumn("subscriptionEnd", 
                   when(col("subscriptionEnd").isNull,to_date(lit("9999-12-31")))
                    .otherwise(col("subscriptionEnd")
                   ))
    
    val joinedDF=user_behavior_df_converted
                .join(subscription_df_converted,Seq("userId"),"inner")

    val filteredDF= joinedDF
            .filter(col("date")
            .between(col("subscriptionStart"),col("subscriptionEnd")))

    val aggDF = filteredDF
                .groupBy("userid")
                .agg(sum("watchduration").alias("totalWatchTime"))
    aggDF
    
}
