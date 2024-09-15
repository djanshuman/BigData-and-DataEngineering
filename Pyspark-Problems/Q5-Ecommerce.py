'''
E-Commerce Platform



You have been given two DataFrames related to an E-commerce platform. The first contains information about the products and their categories, while the second contains information about the orders placed for these products. Write a function that calculates the average price and the total number of orders for each product category.



Input DataFrames:


products_df

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| product_id  | Integer   |
| category    | String    |
| price       | Float     |
+-------------+-----------+


orders_df

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| order_id    | Integer   |
| product_id  | Integer   |
| quantity    | Integer   |
+-------------+-----------+


Output DataFrame:

+--------------------+-----------+
| Column Name        | Data Type |
+--------------------+-----------+
| category           | String    |
| avg_price          | Float     |
| total_orders_count | Integer   |
+--------------------+-----------+


Example



products_df
+------------+----------+-------+
| product_id | category | price |
+------------+----------+-------+
| 1          | Apparel  | 25.99 |
| 2          | Apparel  | 35.99 |
| 3          | Footwear | 50.00 |
| 4          | Footwear | 75.00 |
| 5          | Apparel  | 20.00 |
+------------+----------+-------+

orders_df
+----------+------------+----------+
| order_id | product_id | quantity |
+----------+------------+----------+
| 101      | 1          | 2        |
| 102      | 2          | 1        |
| 103      | 1          | 3        |
| 104      | 3          | 1        |
| 105      | 4          | 2        |
+----------+------------+----------+

Output
+-----------+----------+--------------------+
| avg_price | category | total_orders_count |
+-----------+----------+--------------------+
| 29.323333 | Apparel  | 3                  |
| 62.500000 | Footwear | 2                  |
+-----------+----------+--------------------+

'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(products_df, orders_df):
	# Write code here
	combinedDF= products_df.join(orders_df,on="product_id",how="inner")

  # total orders
  total_orders_df= combinedDF.groupBy("category")\
                    .agg(
                        F.count("order_id").alias("total_orders_count")
                        )
  # avg price orders
  avg_price_df= combinedDF.groupBy("category")\
                    .agg(
                        F.avg("price").alias("avg_price")
                    )
  finalDF= total_orders_df.join(avg_price_df,"category")
  return finalDF

