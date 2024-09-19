'''
Materials Engineering
 
You are working as a Materials Engineer and you are required to process two sets of data related to various experiments conducted in your lab.
 
You need to write a function that performs several join operations. The first DataFrame, df_experiments, has the following schema:

+--------------------+-----------+
|    Column Name     | Data Type |
+--------------------+-----------+
|   experiment_id    |  integer  |
|    material_id     |  integer  |
|  experiment_date   |  string   |
| experiment_results |   float   |
+--------------------+-----------+
 

The second, df_materials, has the following schema:

+---------------+-----------+
|  Column Name  | Data Type |
+---------------+-----------+
|  material_id  |  integer  |
| material_name |  string   |
| material_type |  string   |
+---------------+-----------+
 

Join these two data sets to bring more context to the experiment results. Specifically, you want to join on the material_id field that is common between the two DataFrames. The output should have the following schema:

+--------------------+-----------+
|    Column Name     | Data Type |
+--------------------+-----------+
|   experiment_id    |  integer  |
|    material_id     |  integer  |
|   material_name    |  string   |
|   material_type    |  string   |
|  experiment_date   |  string   |
| experiment_results |   float   |
+--------------------+-----------+
 

The join operation should be such that all records from both the df_experiments and df_materials DataFrame are included, regardless if they have a match on the other DataFrame (full outer join).
  

Example

df_experiments
+---------------+-------------+-----------------+--------------------+
| experiment_id | material_id | experiment_date | experiment_results |
+---------------+-------------+-----------------+--------------------+
|       1       |     101     |   2023-07-01    |        7.6         |
|       2       |     102     |   2023-07-02    |        8.3         |
|       3       |     103     |   2023-07-03    |        6.9         |
|       4       |     101     |   2023-07-04    |        7.2         |
+---------------+-------------+-----------------+--------------------+

df_materials
+-------------+---------------+---------------+
| material_id | material_name | material_type |
+-------------+---------------+---------------+
|     101     |  Material A   |    Type X     |
|     102     |  Material B   |    Type Y     |
|     104     |  Material C   |    Type Z     |
+-------------+---------------+---------------+

Expected
+-----------------+---------------+--------------------+-------------+---------------+---------------+
| experiment_date | experiment_id | experiment_results | material_id | material_name | material_type |
+-----------------+---------------+--------------------+-------------+---------------+---------------+
|   2023-07-01    |      1.0      |        7.6         |     101     |  Material A   |    Type X     |
|   2023-07-02    |      2.0      |        8.3         |     102     |  Material B   |    Type Y     |
|   2023-07-03    |      3.0      |        6.9         |     103     |               |               |
|   2023-07-04    |      4.0      |        7.2         |     101     |  Material A   |    Type X     |
|                 |      nan      |        nan         |     104     |  Material C   |    Type Z     |
+-----------------+---------------+--------------------+-------------+---------------+---------------+
'''


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_experiments, df_materials):
	# Write code here
	return df_experiments.join(df_materials,"material_id","full")
