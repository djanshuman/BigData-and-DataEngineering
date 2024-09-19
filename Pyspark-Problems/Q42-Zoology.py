'''
Zoology
 
In this problem, we are given two DataFrames: AnimalData and RegionData.
 
AnimalData schema:

+-------------+---------+
| Column Name |  Type   |
+-------------+---------+
|     ID      | String  |
|   Species   | String  |
|     Age     | Integer |
|   Weight    |  Float  |
|   Region    | String  |
+-------------+---------+
 

RegionData schema:

+-------------+--------+
| Column Name |  Type  |
+-------------+--------+
|   Region    | String |
|   Climate   | String |
+-------------+--------+

 
Write a function that groups the data by Species, providing the average age and average weight of the animals for each species in each climate.
 
The output should have the following schema:

+--------------+---------+
| Column Name  |  Type   |
+--------------+---------+
|   Species    | String  |
|   Climate    | String  |
|    AvgAge    |  Float  |
|  AvgWeight   | Integer |
| TotalAnimals | Integer |
+--------------+---------+
 

Example

AnimalData
+----+---------+-----+--------+---------------+
| ID | Species | Age | Weight |    Region     |
+----+---------+-----+--------+---------------+
| A1 |  Lion   | 10  | 200.5  |    Africa     |
| A2 |  Tiger  |  5  | 150.3  |     Asia      |
| A3 |  Bear   |  7  | 180.2  | North America |
| A4 |  Lion   | 12  | 205.7  |    Africa     |
| A5 |  Tiger  |  6  | 155.1  |     Asia      |
+----+---------+-----+--------+---------------+

RegionData
+---------------+-----------+
|    Region     |  Climate  |
+---------------+-----------+
|    Africa     |    Hot    |
|     Asia      | Temperate |
| North America |   Cold    |
+---------------+-----------+

Expected
+--------+-----------+-----------+---------+--------------+
| AvgAge | AvgWeight |  Climate  | Species | TotalAnimals |
+--------+-----------+-----------+---------+--------------+
|  11.0  |    203    |    Hot    |  Lion   |      2       |
|  5.5   |    152    | Temperate |  Tiger  |      2       |
|  7.0   |    180    |   Cold    |  Bear   |      1       |
+--------+-----------+-----------+---------+--------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(AnimalData, RegionData):
	# Write code here

    animalDF = AnimalData.groupBy("Species","Region").\
                agg(
                    F.avg("Age").cast("double").alias("AvgAge"),
                    F.avg("Weight").cast("integer").alias("AvgWeight"),
                    F.count("ID").alias("TotalAnimals")
                )
    enrichDF = RegionData.join(animalDF,"Region").\
                select(
                    "AvgAge",
                    "AvgWeight",
                    "Climate",
                    "Species",
                    "TotalAnimals"
                )
    return enrichDF
