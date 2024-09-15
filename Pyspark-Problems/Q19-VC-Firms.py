'''
VC Firms


You are given two DataFrames related to venture capital investments. Write a function that returns the total investment amount in each industry sector and is sorted by total_investment in descending order.

 

 companies

+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| company_id   | integer   |
| company_name | string    |
| industry     | string    |
+--------------+-----------+

investments

+---------------+-----------+
| Column Name   | Data Type |
+---------------+-----------+
| investment_id | integer   |
| company_id    | integer   |
| amount        | double    |
+---------------+-----------+

The output should have the following schema:

+------------------+-----------+
| Column Name      | Data Type |
+------------------+-----------+
| industry         | string    |
| total_investment | double    |
+------------------+-----------+
 

 


Example


companies
+------------+--------------------+------------------+
| company_id | company_name       | industry         |
+------------+--------------------+------------------+
| 1          | AlphaTech          | Technology       |
| 2          | BetaHealth         | Healthcare       |
| 3          | GammaEntertainment | Entertainment    |
| 4          | DeltaGreen         | Renewable Energy |
| 5          | EpsilonFinance     | Finance          |
+------------+--------------------+------------------+

investments
+---------------+------------+---------+
| investment_id | company_id | amount  |
+---------------+------------+---------+
| 1             | 1          | 5000000 |
| 2             | 2          | 3000000 |
| 3             | 3          | 1000000 |
| 4             | 4          | 4000000 |
| 5             | 5          | 2000000 |
+---------------+------------+---------+

Output
+------------------+------------------+
| industry         | total_investment |
+------------------+------------------+
| Entertainment    | 1000000          |
| Finance          | 2000000          |
| Healthcare       | 3000000          |
| Renewable Energy | 4000000          |
| Technology       | 5000000          |
+------------------+------------------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(companies, investments):
	# Write code here
	combinedDF= companies.join(investments,on="company_id",how="inner")
  resultDF= combinedDF.groupBy("industry")\
            .agg(
                F.sum("amount").alias("total_investment")
            ).select("industry","total_investment")\
            .sort(F.desc("total_investment"))

  return resultDF
