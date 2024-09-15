'''
Geology Samples



A geologist is working with a dataset containing information about different rock samples. The dataset is stored in a DataFrame with the following schema:



+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| sample_id   | string    |
| description | string    |
+-------------+-----------+


The description field contains a mixture of letters and numbers. The geologist wants to extract the numeric parts from the description column and create a new column called age.



Write a function that returns the following schema:


+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| sample_id   | string    |
| description | string    |
| age         | string    |
+-------------+-----------+


In the resulting DataFrame, the age column should contain the numeric part extracted from the description column using a regular expression. If there is no numeric part in the description, the age column should contain an empty string.


 Constraints:

The input DataFrame will have at least 1 row and at most 10^4 rows.
The sample_id column will only contain unique alphanumeric strings with 1 to 50 characters.
The description column will contain alphanumeric strings with 1 to 100 characters. The numeric part, if present, will be a positive integer of at most 10^6.


Example




input_df
+-----------+-----------------+
| sample_id | description     |
+-----------+-----------------+
| S1        | Basalt_450Ma    |
| S2        | Sandstone_300Ma |
| S3        | Limestone       |
| S4        | Granite_200Ma   |
| S5        | Marble_1800Ma   |
+-----------+-----------------+

Output
+-----------+-----------------+-----------+
| age       | description     | sample_id |
+-----------+-----------------+-----------+
| Limestone | S3              |           |
| 1800      | Marble_1800Ma   | S5        |
| 200       | Granite_200Ma   | S4        |
| 300       | Sandstone_300Ma | S2        |
| 450       | Basalt_450Ma    | S1        |
+-----------+-----------------+-----------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
  pattern= input_df.withColumn("age",
                        F.regexp_extract("description",r"(\d+)",0))
  return pattern

