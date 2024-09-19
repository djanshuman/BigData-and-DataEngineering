'''
Thermodynamics
 
You are given two DataFrames df_temperature and df_pressure related to some thermodynamics experiments.
 
df_temperature has the following schema:

+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| ExperimentID |  integer  |
| Temperature  |  double   |
+--------------+-----------+
 

df_pressure has the following schema:

+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| ExperimentID |  integer  |
|   Pressure   |  double   |
+--------------+-----------+
 

Output Schema:

+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| ExperimentID |  integer  |
|    Result    |  double   |
+--------------+-----------+
 
 

Write a function that combines these DataFrames and creates the 'Result' column using the formula for the Ideal Gas Law: Pressure * Temperature = Result. For each ExperimentID, multiply the Pressure and Temperature to calculate Result.
 
If there's an ExperimentID present in one DataFrame but not in the other, ignore that ExperimentID. Your solution should only contain ExperimentIDs that exist in both DataFrames.
 
Constraints:
 
The 'ExperimentID' will be unique in each DataFrame.
 
The 'Temperature' and 'Pressure' values are positive and can be assumed to be in appropriate units for the Ideal Gas Law.
 
The 'ExperimentID' in the output DataFrame should be sorted in ascending order.
 

Example

df_temperature
+--------------+-------------+
| ExperimentID | Temperature |
+--------------+-------------+
|     1.0      |   273.15    |
|     2.0      |   293.15    |
|     3.0      |   313.15    |
+--------------+-------------+

df_pressure
+--------------+----------+
| ExperimentID | Pressure |
+--------------+----------+
|     1.0      |   1.0    |
|     3.0      |   2.0    |
|     4.0      |   1.5    |
+--------------+----------+

Expected
+--------------+--------+
| ExperimentID | Result |
+--------------+--------+
|     1.0      | 273.15 |
|     3.0      | 626.3  |
+--------------+--------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_temperature, df_pressure):
	# Write code here
	joinedDF = df_temperature.join(df_pressure,"ExperimentID","inner")

  enrichDF = joinedDF.groupBy("ExperimentID")\
                .agg(
                    F.sum(
                        F.col("Pressure").cast("double") 
                         *
                        F.col("Temperature").cast("double") 
                         ).alias("Result")
                ).orderBy("ExperimentID")
  return enrichDF
    
