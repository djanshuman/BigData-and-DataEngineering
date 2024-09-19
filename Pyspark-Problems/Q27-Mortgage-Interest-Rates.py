'''
Mortgage Interest Rates

 
You are working for a mortgage company which holds records of multiple mortgage types from various users. Each user has a unique ID, and each mortgage type is identified by a unique mortgage ID. The company maintains two different DataFrames, one which logs all the mortgage types and their details MortgageDetails and another which tracks all users and their selected mortgages UserMortgages.
 
Write a function that calculates the interest rate of each type of mortgage.
 
The schemas for the input DataFrames are as follows:
 
 
 MortgageDetails

+--------------+--------+
| Column Name  |  Type  |
+--------------+--------+
|  MortgageID  | String |
| MortgageType | String |
| InterestRate | Double |
+--------------+--------+
 
 
 
UserMortgages
+-------------+--------+
| Column Name |  Type  |
+-------------+--------+
|   UserID    | String |
| MortgageID  | String |
+-------------+--------+
 
Output Schema:

+----------------+--------+
|  Column Name   |  Type  |
+----------------+--------+
|  MortgageType  | String |
| RateOfMortgage | Double |
+----------------+--------+
 

Note: The RateOfMortgage is calculated as the sum of the InterestRate for each MortgageType divided by the count of UserID for each MortgageType.
 
 

Example

MortgageDetails
+------------+--------------+--------------+
| MortgageID | MortgageType | InterestRate |
+------------+--------------+--------------+
|     M1     |    Fixed     |     4.5      |
|     M2     |   Variable   |     3.2      |
|     M3     |  Adjustable  |     2.8      |
+------------+--------------+--------------+

UserMortgages
+--------+------------+
| UserID | MortgageID |
+--------+------------+
|   U1   |     M1     |
|   U2   |     M1     |
|   U3   |     M2     |
|   U4   |     M3     |
+--------+------------+

Expected
+--------------+----------------+
| MortgageType | RateOfMortgage |
+--------------+----------------+
|  Adjustable  |      2.8       |
|    Fixed     |      4.5       |
|   Variable   |      3.2       |
+--------------+----------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(MortgageDetails, UserMortgages):
	# Write code here

    joined_df = MortgageDetails.join(UserMortgages,"MortgageID","inner")
    
    
    agg_df = joined_df.groupBy("MortgageType")\
            .agg(
                F.sum("InterestRate").alias("total_interest"),
                F.count_distinct("UserID").alias("total_user")
            ).withColumn("RateOfMortgage", F.col("total_interest")/F.col("total_user"))\
            .drop("total_user","total_interest")

    return agg_df
