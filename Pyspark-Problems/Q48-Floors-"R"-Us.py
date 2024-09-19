'''
Floors "R" Us
 
You work as a data analyst for a multinational flooring company, "Floors'R'Us". Your task is to process and analyze data from three different sources - the customers, orders, and products tables.
 
The data is structured as follows:
 
The customers DataFrame has the following schema:

+-------------+-----------+
| column_name | data_type |
+-------------+-----------+
| customer_id |    int    |
|  full_name  |  string   |
|  location   |  string   |
+-------------+-----------+
 

The orders DataFrame has the following schema:

+-------------+-----------+
| column_name | data_type |
+-------------+-----------+
|  order_id   |    int    |
| customer_id |    int    |
| product_id  |    int    |
|  quantity   |    int    |
+-------------+-----------+
 

The products DataFrame has the following schema:

+--------------+-----------+
| column_name  | data_type |
+--------------+-----------+
|  product_id  |    int    |
| product_info |  string   |
+--------------+-----------+
 
 
The full_name column in the customers DataFrame is a combination of the customer's first and last name, separated by a space. The product_info column in the products DataFrame contains the type and color of the product, separated by a comma.
 
 
Output Schema: 

+---------------+-----------+
|  column_name  | data_type |
+---------------+-----------+
|   order_id    |    int    |
|  customer_id  |    int    |
|  first_name   |  string   |
|   last_name   |  string   |
|   location    |  string   |
|  product_id   |    int    |
| product_type  |  string   |
| product_color |  string   |
|   quantity    |    int    |
+---------------+-----------+

 
Write a function that splits the full_name column into first_name and last_name columns, and the product_info column into product_type and product_color columns. The result should contain all the orders, each order's associated customer information, and the type and color of the ordered product.
 

Example

customers
+-------------+------------+------------+
| customer_id | full_name  |  location  |
+-------------+------------+------------+
|      1      |  John Doe  |   Texas    |
|      2      | Jane Smith | California |
+-------------+------------+------------+

orders
+----------+-------------+------------+----------+
| order_id | customer_id | product_id | quantity |
+----------+-------------+------------+----------+
|   1001   |      1      |    101     |    5     |
|   1002   |      2      |    102     |    2     |
+----------+-------------+------------+----------+

products
+------------+--------------+
| product_id | product_info |
+------------+--------------+
|    101     |  Carpet,Red  |
|    102     |  Tile,Blue   |
+------------+--------------+

Expected
+-------------+------------+-----------+------------+----------+---------------+------------+--------------+----------+
| customer_id | first_name | last_name |  location  | order_id | product_color | product_id | product_type | quantity |
+-------------+------------+-----------+------------+----------+---------------+------------+--------------+----------+
|      1      |    John    |    Doe    |   Texas    |   1001   |      Red      |    101     |    Carpet    |    5     |
|      2      |    Jane    |   Smith   | California |   1002   |     Blue      |    102     |     Tile     |    2     |
+-------------+------------+-----------+------------+----------+---------------+------------+--------------+----------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(customers, orders, products):
	# Write code here


    customers = customers.withColumn("first_name",
                        F.split(F.col("full_name")," ").getItem(0))\
                        .withColumn("last_name",
                        F.split(F.col("full_name")," ").getItem(1))
                        
    customers_joinedDF = customers.join(orders,"customer_id","inner")\
                    .join(products,"product_id","inner")

    
    customers_joinedDF = customers_joinedDF.withColumn("product_type",
                        F.split(F.col("product_info"),",").getItem(0))\
                        .withColumn("product_color",
                        F.split(F.col("product_info"),",").getItem(1))

    return customers_joinedDF.select(
                    "customer_id",
                   "first_name",
                    "last_name",
                    "location",
                    "order_id",
                    "product_color",
                    "product_id",
                    "product_type",
                    "quantity")
