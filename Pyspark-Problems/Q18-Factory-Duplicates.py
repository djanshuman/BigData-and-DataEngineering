'''
Factory Duplicates


In a manufacturing company, they collect data about their products and manufacturing processes. You are given two DataFrames. The first contains information about the products, and the second contains information about the manufacturing processes.


Write a function that removes duplicates from both DataFrames then combines them on their Product ID.


products_df

+-------------+-----------+------------------------------------+
| Column Name | Data Type | Description                        |
+-------------+-----------+------------------------------------+
| ProductID   | integer   | Unique identifier for each product |
| ProductName | string    | Name of the product                |
| Category    | string    | Category of the product            |
+-------------+-----------+------------------------------------+

manufacturing_processes_df

+-------------+-----------+--------------------------------------------------------+
| Column Name | Data Type | Description                                            |
+-------------+-----------+--------------------------------------------------------+
| ProcessID   | integer   | Unique identifier for each manufacturing process       |
| ProductID   | integer   | Identifier for the product associated with the process |
| ProcessName | string    | Name of the manufacturing process                      |
| Duration    | float     | Duration of the process in hours                       |
+-------------+-----------+--------------------------------------------------------+

Output DataFrame schema:

+-------------+-----------+--------------------------------------------------+
| Column Name | Data Type | Description                                      |
+-------------+-----------+--------------------------------------------------+
| ProductID   | integer   | Unique identifier for each product               |
| ProductName | string    | Name of the product                              |
| Category    | string    | Category of the product                          |
| ProcessID   | integer   | Unique identifier for each manufacturing process |
| ProcessName | string    | Name of the manufacturing process                |
| Duration    | float     | Duration of the process in hours                 |
+-------------+-----------+--------------------------------------------------+
 

 


Example


products_df
+-----------+-------------+----------+
| ProductID | ProductName | Category |
+-----------+-------------+----------+
| 1         | Widget A    | Type1    |
| 2         | Widget B    | Type1    |
| 3         | Widget C    | Type2    |
| 4         | Widget D    | Type2    |
| 1         | Widget A    | Type1    |
+-----------+-------------+----------+

manufacturing_processes_df
+-----------+-----------+-------------+----------+
| ProcessID | ProductID | ProcessName | Duration |
+-----------+-----------+-------------+----------+
| 1001      | 1         | Cutting     | 1.5      |
| 1002      | 2         | Cutting     | 1.6      |
| 1003      | 3         | Cutting     | 1.8      |
| 1004      | 4         | Cutting     | 1.5      |
| 1005      | 1         | Shaping     | 2.0      |
+-----------+-----------+-------------+----------+

Output
+----------+----------+-----------+-------------+-----------+-------------+
| Category | Duration | ProcessID | ProcessName | ProductID | ProductName |
+----------+----------+-----------+-------------+-----------+-------------+
| Type1    | 1.5      | 1001      | Cutting     | 1         | Widget A    |
| Type1    | 1.6      | 1002      | Cutting     | 2         | Widget B    |
| Type1    | 2.0      | 1005      | Shaping     | 1         | Widget A    |
| Type2    | 1.5      | 1004      | Cutting     | 4         | Widget D    |
| Type2    | 1.8      | 1003      | Cutting     | 3         | Widget C    |
+----------+----------+-----------+-------------+-----------+-------------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(products_df, manufacturing_processes_df):
	# Write code here
	clean_products_df= products_df.dropDuplicates(["ProductID"])
  clean_manufacturing_processes_df = manufacturing_processes_df\
                .dropDuplicates(["ProcessID","ProductID"])
  resultDF= clean_products_df.join(clean_manufacturing_processes_df,on="ProductID",how="inner")
  return resultDF
