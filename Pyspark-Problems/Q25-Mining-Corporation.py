'''
Mining Corporation

A mining company extracts rare minerals from various locations. It maintains two DataFrames to track its operations.
 
The first, mines, keeps track of each mine with the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|     id      |  Integer  |
|    name     |  String   |
|  location   |  String   |
+-------------+-----------+

where:
id is the unique identifier of the mine.
name is the name of the mine.
location is the place where the mine is located.
 
The second, extraction, contains information about the extracted minerals with the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|   mine_id   |  Integer  |
|    date     |   Date    |
|   mineral   |  String   |
|  quantity   |  Integer  |
+-------------+-----------+

where:
mine_id is the unique identifier of the mine from where the mineral was extracted.
date is the extraction date.
mineral is the name of the extracted mineral.
quantity is the quantity of the mineral extracted on the date in kilograms.
 
Write a function that shows the total quantity of each mineral extracted per location. The result should have the following schema:

+----------------+-----------+
|  Column Name   | Data Type |
+----------------+-----------+
|    location    |  String   |
|    mineral     |  String   |
| total_quantity |  Double   |
+----------------+-----------+

The total_quantity column should contain the sum of all quantities of a particular mineral extracted at a particular location. The rows should be sorted first by location (in ascending order) and then by mineral (in ascending order).
 

Expected

mines
+----+------------+--------------+
| id |    name    |   location   |
+----+------------+--------------+
| 1  | Mine Alpha |  Australia   |
| 2  | Mine Beta  |    Canada    |
| 3  | Mine Gamma | South Africa |
+----+------------+--------------+

extraction
+---------+------------+---------+----------+
| mine_id |    date    | mineral | quantity |
+---------+------------+---------+----------+
|    1    | 2023-06-30 |  Gold   |  1000.0  |
|    2    | 2023-06-30 | Silver  |  1200.0  |
|    3    | 2023-06-30 | Diamond |  800.0   |
|    1    | 2023-06-29 |  Gold   |  900.0   |
|    2    | 2023-06-29 | Silver  |  1300.0  |
|    3    | 2023-06-29 | Diamond |  750.0   |
+---------+------------+---------+----------+

Expected
+--------------+---------+----------------+
|   location   | mineral | total_quantity |
+--------------+---------+----------------+
|  Australia   |  Gold   |      1900      |
|    Canada    | Silver  |      2500      |
| South Africa | Diamond |      1550      |
+--------------+---------+----------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(mines, extraction):
	# Write code here

    #return df_agg_extr
    df_final_agg = mines.join(extraction,mines.id == extraction.mine_id)
    
    df_agg_extr = df_final_agg.groupBy("location","mineral")\
                    .agg(
                        F.sum("quantity").alias("total_quantity")
                    ).orderBy("location","mineral")
    return df_agg_extr
