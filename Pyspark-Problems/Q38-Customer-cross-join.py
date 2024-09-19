'''
Customer Cross Join
 
A bank maintains its transactions and customer data in separate databases. However, for certain data analysis tasks, the bank needs to perform a cross join operation between the 'transactions' and 'customers' datasets.
 
Write a function that performs a cross join operation on these two DataFrames.
 
Input
 
transactions has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|  trans_id   |  Integer  |
|  trans_amt  |   Float   |
|    date     |  String   |
|   cust_id   |  Integer  |
+-------------+-----------+
 
customers has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|   cust_id   |  Integer  |
| first_name  |  String   |
|  last_name  |  String   |
|     age     |  Integer  |
+-------------+-----------+
 
 
Output Schema:
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|  trans_id   |  Integer  |
|  trans_amt  |   Float   |
|    date     |  String   |
|   cust_id   |  Integer  |
| first_name  |  String   |
|  last_name  |  String   |
|     age     |  Integer  |
+-------------+-----------+
 

Example

transactions
+----------+-----------+------------+---------+
| trans_id | trans_amt |    date    | cust_id |
+----------+-----------+------------+---------+
|    1     |   500.0   | 2023-07-01 |  1001   |
|    2     |   200.0   | 2023-07-02 |  1002   |
|    3     |   300.0   | 2023-07-03 |  1003   |
+----------+-----------+------------+---------+

customers
+---------+------------+-----------+-----+
| cust_id | first_name | last_name | age |
+---------+------------+-----------+-----+
|  1001   |    John    |    Doe    | 30  |
|  1002   |    Jane    |   Smith   | 40  |
|  1003   |    Bob     |  Johnson  | 50  |
+---------+------------+-----------+-----+

Expected
+-----+---------+------------+------------+-----------+-----------+----------+
| age | cust_id |    date    | first_name | last_name | trans_amt | trans_id |
+-----+---------+------------+------------+-----------+-----------+----------+
| 30  |  1001   | 2023-07-01 |    John    |    Doe    |    500    |    1     |
| 30  |  1001   | 2023-07-02 |    John    |    Doe    |    200    |    2     |
| 30  |  1001   | 2023-07-03 |    John    |    Doe    |    300    |    3     |
| 40  |  1002   | 2023-07-01 |    Jane    |   Smith   |    500    |    1     |
| 40  |  1002   | 2023-07-02 |    Jane    |   Smith   |    200    |    2     |
| 40  |  1002   | 2023-07-03 |    Jane    |   Smith   |    300    |    3     |
| 50  |  1003   | 2023-07-01 |    Bob     |  Johnson  |    500    |    1     |
| 50  |  1003   | 2023-07-02 |    Bob     |  Johnson  |    200    |    2     |
| 50  |  1003   | 2023-07-03 |    Bob     |  Johnson  |    300    |    3     |
+-----+---------+------------+------------+-----------+-----------+----------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(transactions, customers):
	# Write code here
    return transactions.crossJoin(customers)
