'''
Funded Startups
 

We have two DataFrames containing information about Venture Capitalists and the start-ups they have funded. The aim is to find Venture Capitalists that have funded start-ups that have an average funding above a certain limit, the limit is different for each Venture Capitalist.
 
The first venture_capitalist_df contains the following schema:

+---------------+--------+
|  Column Name  |  Type  |
+---------------+--------+
|     vc_id     | string |
|    vc_name    | string |
| funding_limit | float  |
+---------------+--------+

 
The second funded_startups_df contains the following schema:

+--------------+--------+
| Column Name  |  Type  |
+--------------+--------+
|  startup_id  | string |
| startup_name | string |
|    vc_id     | string |
|   funding    | float  |
+--------------+--------+

 
The vc_id column in venture_capitalist_df corresponds to the vc_id column in funded_startups_df.
 
 
Output Schema:
+-------------+--------+
| Column Name |  Type  |
+-------------+--------+
|    vc_id    | string |
|   vc_name   | string |
| avg_funding | float  |
+-------------+--------+

 
Write a function that combines these DataFrames and returns the Venture Capitalists whose funded start-ups have an average funding amount above their corresponding funding limit. The avg_funding field should contain the average funding provided by the Venture Capitalist to the startups.
 
 

Example

venture_capitalist_df
+-------+-----------+---------------+
| vc_id |  vc_name  | funding_limit |
+-------+-----------+---------------+
|  VC1  | VC Firm 1 |      1.5      |
|  VC2  | VC Firm 2 |      2.0      |
|  VC3  | VC Firm 3 |     1.75      |
|  VC4  | VC Firm 4 |      2.5      |
+-------+-----------+---------------+

funded_startups_df
+------------+--------------+-------+---------+
| startup_id | startup_name | vc_id | funding |
+------------+--------------+-------+---------+
|     S1     |  Startup 1   |  VC1  |   2.0   |
|     S2     |  Startup 2   |  VC1  |   1.0   |
|     S3     |  Startup 3   |  VC2  |   2.5   |
|     S4     |  Startup 4   |  VC2  |   2.0   |
|     S5     |  Startup 5   |  VC3  |   1.8   |
|     S6     |  Startup 6   |  VC3  |   1.7   |
|     S7     |  Startup 7   |  VC4  |   3.0   |
|     S8     |  Startup 8   |  VC4  |   2.0   |
+------------+--------------+-------+---------+

Expected
+-------------+---------------+-------+-----------+
| avg_funding | funding_limit | vc_id |  vc_name  |
+-------------+---------------+-------+-----------+
|    2.25     |       2       |  VC2  | VC Firm 2 |
+-------------+---------------+-------+-----------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(venture_capitalist_df, funded_startups_df):
	# Write code here

    # avg funding per vc_id

    avg_funding_per_vc = funded_startups_df.groupBy("vc_id")\
                        .agg(
                            F.avg("funding").alias("avg_funding")
                        )
    joinedDF= avg_funding_per_vc\
                    .join(venture_capitalist_df,"vc_id","inner")\
                    .withColumn("Flag", F.col("avg_funding") > F.col("funding_limit")
                               ).filter(F.col("Flag")).drop("Flag")
    return joinedDF
