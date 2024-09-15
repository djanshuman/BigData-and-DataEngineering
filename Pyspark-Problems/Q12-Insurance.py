'''
Insurance Customers



An insurance agency wants to merge their customer data, which is stored in two separate DataFrames. Write a function that returns all rows from both input DataFrames.



input_df1 Schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| customer_id | Integer   |
| first_name  | String    |
| last_name   | String    |
| age         | Integer   |
| policy_type | String    |
+-------------+-----------+


input_df2 Schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| customer_id | Integer   |
| first_name  | String    |
| last_name   | String    |
| age         | Integer   |
| policy_type | String    |
+-------------+-----------+


Output DataFrame Schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| customer_id | Integer   |
| first_name  | String    |
| last_name   | String    |
| age         | Integer   |
| policy_type | String    |
+-------------+-----------+


Example



input_df1
+-------------+------------+-----------+-----+-------------+
| customer_id | first_name | last_name | age | policy_type |
+-------------+------------+-----------+-----+-------------+
| 1           | Alice      | Smith     | 30  | auto        |
| 2           | Bob        | Johnson   | 40  | home        |
| 3           | Carol      | Williams  | 35  | life        |
+-------------+------------+-----------+-----+-------------+

input_df2
+-------------+------------+-----------+-----+-------------+
| customer_id | first_name | last_name | age | policy_type |
+-------------+------------+-----------+-----+-------------+
| 4           | Dave       | Brown     | 45  | auto        |
| 5           | Eve        | Jones     | 55  | health      |
| 6           | Frank      | Davis     | 60  | life        |
+-------------+------------+-----------+-----+-------------+

Output
+-----+-------------+------------+-----------+-------------+
| age | customer_id | first_name | last_name | policy_type |
+-----+-------------+------------+-----------+-------------+
| 30  | 1           | Alice      | Smith     | auto        |
| 35  | 3           | Carol      | Williams  | life        |
| 40  | 2           | Bob        | Johnson   | home        |
| 45  | 4           | Dave       | Brown     | auto        |
| 55  | 5           | Eve        | Jones     | health      |
| 60  | 6           | Frank      | Davis     | life        |
+-----+-------------+------------+-----------+-------------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df1, input_df2):
	
    # Write code here
	  resultDF= input_df1.unionAll(input_df2)
    return resultDF
