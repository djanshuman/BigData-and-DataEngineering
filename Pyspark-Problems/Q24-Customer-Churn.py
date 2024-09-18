'''
Customer Churn
 
A top tech company is facing a challenge with user churn. They maintain three different sources of data: user accounts, user activities and user exit surveys. All these data sources are represented as three separate DataFrames: df_accounts, df_activities and df_exit_surveys.
 
df_accounts consists of the following schema:

+----------------------+--------+
|     Column Name      |  Type  |
+----------------------+--------+
|       user_id        | String |
| account_created_date |  Date  |
|       location       | String |
+----------------------+--------+

df_activities has the following schema:

+---------------+--------+
|  Column Name  |  Type  |
+---------------+--------+
|    user_id    | String |
| activity_date |  Date  |
| activity_type | String |
+---------------+--------+

df_exit_surveys has the following schema:

+-------------+--------+
| Column Name |  Type  |
+-------------+--------+
|   user_id   | String |
|  exit_date  |  Date  |
| exit_reason | String |
+-------------+--------+

Unfortunately, due to some systems glitches, there are duplicates within the df_activities DataFrame. The definition of duplicates for this DataFrame are rows that have the exact same user_id, activity_date and activity_type.
 
Write a function that combines the 3 DataFrames and resolves the duplicates in df_activities and preserves the original order of records. 
 
The output DataFrame will have the following schema:

+----------------------+--------+
|     Column Name      |  Type  |
+----------------------+--------+
|       user_id        | String |
| account_created_date |  Date  |
|       location       | String |
|    activity_date     |  Date  |
|    activity_type     | String |
|      exit_date       |  Date  |
|     exit_reason      | String |
+----------------------+--------+

The data should be sorted by user_id in ascending order and then by activity_date in descending order. If there is no corresponding row for a user in any of the input DataFrames, the respective columns should contain null values in the output DataFrame.

Expected

df_accounts
+---------+----------------------+---------------+
| user_id | account_created_date |   location    |
+---------+----------------------+---------------+
|  U001   |      2023-01-01      |   New York    |
|  U002   |      2023-01-05      |    Chicago    |
|  U003   |      2023-01-10      | San Francisco |
+---------+----------------------+---------------+

df_activities
+---------+---------------+---------------+
| user_id | activity_date | activity_type |
+---------+---------------+---------------+
|  U001   |  2023-02-01   |     Login     |
|  U001   |  2023-02-01   |     Login     |
|  U002   |  2023-02-05   |  File Upload  |
|  U002   |  2023-02-05   |  File Upload  |
|  U003   |  2023-02-10   |    Logout     |
+---------+---------------+---------------+

df_exit_surveys
+---------+------------+-----------------------+
| user_id | exit_date  |      exit_reason      |
+---------+------------+-----------------------+
|  U001   | 2023-03-01 | Moved to a competitor |
|  U002   | 2023-03-05 |   Not user-friendly   |
|  U003   | 2023-03-10 |     High pricing      |
+---------+------------+-----------------------+

Expected
+----------------------+---------------+---------------+------------+-----------------------+---------------+---------+
| account_created_date | activity_date | activity_type | exit_date  |      exit_reason      |   location    | user_id |
+----------------------+---------------+---------------+------------+-----------------------+---------------+---------+
|      2023-01-01      |  2023-02-01   |     Login     | 2023-03-01 | Moved to a competitor |   New York    |  U001   |
|      2023-01-05      |  2023-02-05   |  File Upload  | 2023-03-05 |   Not user-friendly   |    Chicago    |  U002   |
|      2023-01-10      |  2023-02-10   |    Logout     | 2023-03-10 |     High pricing      | San Francisco |  U003   |
+----------------------+---------------+---------------+------------+-----------------------+---------------+---------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_accounts, df_activities, df_exit_surveys):
	# Write code here

    df_activities_cleaned = df_activities.dropDuplicates \
                                    (["user_id","activity_date","activity_type"])

    df_consolidates = df_accounts\
                    .join(df_activities_cleaned,on="user_id",how="full")\
                    .join(df_exit_surveys,on='user_id',how='full')\
                    .select(
                        "user_id",
                        "account_created_date",
                        "location",
                        "activity_date",
                        "activity_type",
                        "exit_date",
                        "exit_reason"
                    ).orderBy("user_id",F.desc("activity_date"))
    return df_consolidates
	
