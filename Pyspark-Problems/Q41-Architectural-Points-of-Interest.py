'''
Architectural Points of Interest
 
In this problem, you will be working with a DataFrame that represents a set of architectural structures. Each row in the dataset represents a unique building and includes various details about its architecture.
 
Below is the schema of buildings:

+-------------+---------+
| Column Name |  Type   |
+-------------+---------+
|     id      | Integer |
|    name     | String  |
|    city     | String  |
|   country   | String  |
|  height_m   |  Float  |
|   floors    | Integer |
+-------------+---------+

 
Write a function that calculates the average height per floor for each building, with the result rounded to two decimal places. The result should include the id, name, city, country, and the calculated average height per floor (named avg_height_per_floor). If the number of floors is zero, the average height should also be zero.
 
Here is the schema of the output:

+----------------------+---------+
|     Column Name      |  Type   |
+----------------------+---------+
|          id          | Integer |
|         name         | String  |
|         city         | String  |
|       country        | String  |
| avg_height_per_floor |  Float  |
+----------------------+---------+
 

Example

buildings
+----+---------------------------+----------+--------------+----------+--------+
| id |           name            |   city   |   country    | height_m | floors |
+----+---------------------------+----------+--------------+----------+--------+
| 1  |  One World Trade Center   | New York |     USA      |  541.3   |  104   |
| 2  |       Willis Tower        | Chicago  |     USA      |  442.1   |  108   |
| 3  |       Burj Khalifa        |  Dubai   |     UAE      |  828.0   |  163   |
| 4  |         The Shard         |  London  |      UK      |  309.6   |   72   |
| 5  | Abraj Al-Bait Clock Tower |  Mecca   | Saudi Arabia |  601.0   |  120   |
+----+---------------------------+----------+--------------+----------+--------+

Expected
+----------------------+----------+--------------+----+---------------------------+
| avg_height_per_floor |   city   |   country    | id |           name            |
+----------------------+----------+--------------+----+---------------------------+
|         4.09         | Chicago  |     USA      | 2  |       Willis Tower        |
|         4.3          |  London  |      UK      | 4  |         The Shard         |
|         5.01         |  Mecca   | Saudi Arabia | 5  | Abraj Al-Bait Clock Tower |
|         5.08         |  Dubai   |     UAE      | 3  |       Burj Khalifa        |
|         5.2          | New York |     USA      | 1  |  One World Trade Center   |
+----------------------+----------+--------------+----+---------------------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(buildings):
	# Write code here
	pass

    condition = (F.when(
            F.col("floors")!= 0,
            F.round(F.col("height_m") / F.col("floors"),2)
        ).otherwise(0))

    newDF= buildings.withColumn("avg_height_per_floor",condition)\
                .select(
                    "avg_height_per_floor",
                    "city",
                    "country",
                    "id",
                    "name")
    return newDF
