'''
Organizing Manufacturing Parts
 
A manufacturing company is analyzing its production process. You are provided with two DataFrames:
 
df1 Schema:

+------------------------+-----------+
|      Column Name       | Data Type |
+------------------------+-----------+
|       product_id       |  String   |
|   manufacturing_date   |   Date    |
| manufacturing_location |  String   |
+------------------------+-----------+
 

df2 Schema:

+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
|  product_id  |  String   |
| product_name |  String   |
| product_type |  String   |
+--------------+-----------+
 

The product_id in df1 corresponds to the product_id in df2.
 
Write a function that joins both the DataFrames on the product_id. After joining the DataFrames, create a new column named row_number which assigns a unique serial number to each row in the ascending order of manufacturing_date. The row_number should start from 1 and increment by 1 for every subsequent row.
 
 
Output DataFrame Schema:

+------------------------+-----------+
|      Column Name       | Data Type |
+------------------------+-----------+
|       product_id       |  String   |
|   manufacturing_date   |   Date    |
| manufacturing_location |  String   |
|      product_name      |  String   |
|      product_type      |  String   |
|       row_number       |  Integer  |
+------------------------+-----------+
 

Example

df1
+------------+--------------------+------------------------+
| product_id | manufacturing_date | manufacturing_location |
+------------+--------------------+------------------------+
|     P1     |     2023-01-01     |       Location_A       |
|     P2     |     2023-01-02     |       Location_B       |
|     P3     |     2023-01-03     |       Location_C       |
+------------+--------------------+------------------------+

df2
+------------+--------------+--------------+
| product_id | product_name | product_type |
+------------+--------------+--------------+
|     P1     |   Widget_A   |    Widget    |
|     P2     |   Gadget_B   |    Gadget    |
|     P3     |   Device_C   |    Device    |
+------------+--------------+--------------+

Expected
+--------------------+------------------------+------------+--------------+--------------+------------+
| manufacturing_date | manufacturing_location | product_id | product_name | product_type | row_number |
+--------------------+------------------------+------------+--------------+--------------+------------+
|     2023-01-01     |       Location_A       |     P1     |   Widget_A   |    Widget    |     1      |
|     2023-01-02     |       Location_B       |     P2     |   Gadget_B   |    Gadget    |     2      |
|     2023-01-03     |       Location_C       |     P3     |   Device_C   |    Device    |     3      |
+--------------------+------------------------+------------+--------------+--------------+------------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
	# Write code here
	new_df = df1.join(df2,"product_id","inner")
    window_spec = W.orderBy("manufacturing_date")

    final_df= new_df.select(
        "manufacturing_date",
        "manufacturing_location",
        "product_id",
        "product_name",
        "product_type",
        F.row_number().over(window_spec).alias("row_number")
    )
    return final_df
