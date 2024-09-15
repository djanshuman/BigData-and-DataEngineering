'''
Call Center

 

You are given two DataFrames, calls_df and customers_df, which contain information about calls made by customers of a telecommunications company and information about the customers, respectively.

 

calls_df has the following schema:

 

+----------+---------+
| Column   | Type    |
+----------+---------+
| call_id  | integer |
| cust_id  | integer |
| date     | string  |
| duration | integer |
+----------+---------+
 

where:

call_id is the unique identifier of each call.
cust_id is the unique identifier of the customer who made the call.
date is the date when the call was made in the format "yyyy-MM-dd".
duration is the duration of the call in seconds.
customers_df has the following schema:

 

+------------+---------+
| Column     | Type    |
+------------+---------+
| cust_id    | integer |
| name       | string  |
| state      | string  |
| tenure     | integer |
| occupation | string  |
+------------+---------+
 

where:

cust_id is the unique identifier of each customer.
name is the name of the customer.
state is the state where the customer lives.
tenure is the number of months the customer has been with the company.
occupation is the occupation of the customer.
 

 

Write a function that returns the number of distinct customers who made calls on each date, along with the total duration of calls made on each date.

 

The output DataFrame should have the following schema:

 

+----------------+---------+
| Column         | Type    |
+----------------+---------+
| date           | string  |
| num_customers  | integer |
| total_duration | integer |
+----------------+---------+
 

where:

date is the date when the calls were made in the format "yyyy-MM-dd".
num_customers is the number of distinct customers who made calls on that date.
total_duration is the total duration of calls made on that date in seconds.
 

You may assume that the upstream DataFrames are not empty.

Example


calls_df
+---------+---------+------------+----------+
| call_id | cust_id | date       | duration |
+---------+---------+------------+----------+
| 1       | 1       | 2022-01-01 | 100      |
| 2       | 2       | 2022-01-01 | 200      |
| 3       | 1       | 2022-01-02 | 150      |
| 4       | 3       | 2022-01-02 | 300      |
| 5       | 2       | 2022-01-03 | 50       |
+---------+---------+------------+----------+

customers_df
+---------+---------+-------+--------+------------+
| cust_id | name    | state | tenure | occupation |
+---------+---------+-------+--------+------------+
| 1       | Alice   | NY    | 10     | doctor     |
| 2       | Bob     | CA    | 12     | lawyer     |
| 3       | Charlie | TX    | 6      | engineer   |
+---------+---------+-------+--------+------------+


Output
+------------+---------------+----------------+
| date       | num_customers | total_duration |
+------------+---------------+----------------+
| 2022-01-01 | 2             | 300            |
| 2022-01-02 | 2             | 450            |
| 2022-01-03 | 1             | 50             |
+------------+---------------+----------------+

'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(calls_df, customers_df):
	# Write code here
  combinedDF= calls_df.join(customers_df,"cust_id")

  agg_df= combinedDF.groupBy("date")\
            .agg(F.sum("duration").alias("total_duration"),
                 F.count_distinct("cust_id").alias("num_customers"))
  return agg_df

    
	
