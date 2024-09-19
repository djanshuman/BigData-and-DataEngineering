'''
Camping Supplies
 
You are working for an outdoor supplies company that sells various items such as camping equipment, hiking gear, fishing equipment etc. You are given two DataFrames.
 
The first, df_sales, has the following schema:

+---------------+---------+
|  Column Name  |  Type   |
+---------------+---------+
|   sales_id    | String  |
|  product_id   | String  |
|     date      |  Date   |
| quantity_sold | Integer |
+---------------+---------+

 
The second, df_products, has the following schema:

+------------------+--------+
|   Column Name    |  Type  |
+------------------+--------+
|    product_id    | String |
|   product_name   | String |
| product_category | String |
+------------------+--------+
 

Write a function that returns the total quantity sold for each product category on a daily basis and has the following schema:

+------------------+---------+
|   Column Name    |  Type   |
+------------------+---------+
|       date       |  Date   |
| product_category | String  |
|  total_quantity  | Integer |
+------------------+---------+
 

Example

df_sales
+----------+------------+------------+---------------+
| sales_id | product_id |    date    | quantity_sold |
+----------+------------+------------+---------------+
|    S1    |     P1     | 2023-06-01 |      10       |
|    S2    |     P2     | 2023-06-02 |      15       |
|    S3    |     P3     | 2023-06-02 |      20       |
|    S4    |     P4     | 2023-06-01 |      12       |
|    S5    |     P5     | 2023-06-03 |      25       |
+----------+------------+------------+---------------+

df_products
+------------+------------------+------------------+
| product_id |   product_name   | product_category |
+------------+------------------+------------------+
|     P1     |   Camping Tent   |     Camping      |
|     P2     |   Hiking Shoes   |      Hiking      |
|     P3     |   Fishing Rod    |     Fishing      |
|     P4     | Insulated Bottle |      Hiking      |
|     P5     |  Outdoor Grill   |     Camping      |
+------------+------------------+------------------+

Expected
+------------+------------------+----------------+
|    date    | product_category | total_quantity |
+------------+------------------+----------------+
| 2023-06-01 |     Camping      |       10       |
| 2023-06-01 |      Hiking      |       12       |
| 2023-06-02 |     Fishing      |       20       |
| 2023-06-02 |      Hiking      |       15       |
| 2023-06-03 |     Camping      |       25       |
+------------+------------------+----------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_sales, df_products):
	# Write code here
    joinedDF = df_sales.join(df_products,"product_id","inner")

    final_df = joinedDF.groupBy("product_category","date")\
                .agg(
                    F.sum("quantity_sold").alias("total_quantity")
                )
    return final_df
