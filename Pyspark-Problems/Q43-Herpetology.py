'''
Herpetology
 
As a herpetologist studying reptiles and amphibians, you have two DataFrames at your disposal: observations and species.
 
observations consists of the following fields:

+-------------+-----------+----------------------------------------------------------------------+
| Column Name | Data Type |                             Description                              |
+-------------+-----------+----------------------------------------------------------------------+
|   obs_id    |  Integer  |               The unique identifier of the observation               |
| species_id  |  Integer  |            The unique identifier of the species observed             |
| location_id |  Integer  | The unique identifier of the location where the observation was made |
|    count    |  Integer  |                  The number of individuals observed                  |
+-------------+-----------+----------------------------------------------------------------------+
 

species consists of the following fields:

+--------------+-----------+--------------------------------------+
| Column Name  | Data Type |             Description              |
+--------------+-----------+--------------------------------------+
|  species_id  |  Integer  | The unique identifier of the species |
| species_name |  String   |    The common name of the species    |
+--------------+-----------+--------------------------------------+

 
Write a function that joins observations and species on the species_id column, and then returns the top 3 rows ordered by count in descending order. The output schema should be as follows:

+--------------+-----------+----------------------------------------------------------------------+
| Column Name  | Data Type |                             Description                              |
+--------------+-----------+----------------------------------------------------------------------+
|    obs_id    |  Integer  |               The unique identifier of the observation               |
|  species_id  |  Integer  |                 The unique identifier of the species                 |
| species_name |  String   |                    The common name of the species                    |
| location_id  |  Integer  | The unique identifier of the location where the observation was made |
|    count     |  Integer  |                  The number of individuals observed                  |
+--------------+-----------+----------------------------------------------------------------------+
 

Example

observations
+--------+------------+-------------+-------+
| obs_id | species_id | location_id | count |
+--------+------------+-------------+-------+
|   1    |    100     |      1      |  55   |
|   2    |    101     |      2      |  35   |
|   3    |    100     |      1      |  45   |
+--------+------------+-------------+-------+

species
+------------+--------------+
| species_id | species_name |
+------------+--------------+
|    100     |    Python    |
|    101     |    Gecko     |
|    102     |     Frog     |
+------------+--------------+

Expected
+-------+-------------+--------+------------+--------------+
| count | location_id | obs_id | species_id | species_name |
+-------+-------------+--------+------------+--------------+
|  35   |      2      |   2    |    101     |    Gecko     |
|  45   |      1      |   3    |    100     |    Python    |
|  55   |      1      |   1    |    100     |    Python    |
+-------+-------------+--------+------------+--------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(observations, species):
	# Write code here
    window_spec = W.orderBy(F.desc("count"))

    
	finalDF= observations.join(species,"species_id","inner")\
                          .withColumn("rnk",
                           F.rank().over(window_spec))
    
  return finalDF.filter(F.col("rnk")<=3).drop("rnk")
