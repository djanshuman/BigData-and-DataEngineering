'''
Retail Stores


In a retail store, we often keep track of the customer transactions to maintain inventory, sales and customer data. The data often contains customer information, purchase details and transaction date.


transactions has the following schema:


+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| customer_id | string    |
| product_id  | string    |
| quantity    | integer   |
| date        | string    |
+-------------+-----------+


The date field contains the date of transaction in the format 'YYYY-MM-DD'. The customer_id field contains the unique ID of a customer, the product_id contains the unique ID of a product and quantity contains the quantity of the product purchased in the transaction.


Write a function that computes the following: for each row, we add a new column previous_product that contains the product_id of the previous transaction made by the same customer, in chronological order. If there's no previous transaction for a customer, the previous_product column should contain None. Also, the date and previous_product columns should be concatenated into a new column date_and_product, separated by a space.



The output DataFrame should have the following schema:


+------------------+-----------+
| Column Name      | Data Type |
+------------------+-----------+
| customer_id      | string    |
| product_id       | string    |
| quantity         | integer   |
| date             | string    |
| previous_product | string    |
| date_and_product | string    |
+------------------+-----------+
 
Example
transactions
+-------------+------------+----------+------------+
| customer_id | product_id | quantity | date       |
+-------------+------------+----------+------------+
| CUST1       | PROD1      | 2        | 2023-01-01 |
| CUST1       | PROD2      | 1        | 2023-01-05 |
| CUST2       | PROD3      | 5        | 2023-02-03 |
| CUST3       | PROD1      | 4        | 2023-02-07 |
| CUST1       | PROD3      | 3        | 2023-02-10 |
+-------------+------------+----------+------------+

Expected
+-------------+------------+------------------+------------------+------------+----------+
| customer_id | date       | date_and_product | previous_product | product_id | quantity |
+-------------+------------+------------------+------------------+------------+----------+
| CUST1       | 2023-01-01 | 2023-01-01 None  | None             | PROD1      | 2        |
| CUST1       | 2023-01-05 | 2023-01-05 PROD1 | PROD1            | PROD2      | 1        |
| CUST1       | 2023-02-10 | 2023-02-10 PROD2 | PROD2            | PROD3      | 3        |
| CUST2       | 2023-02-03 | 2023-02-03 None  | None             | PROD3      | 5        |
| CUST3       | 2023-02-07 | 2023-02-07 None  | None             | PROD1      | 4        |
+-------------+------------+------------------+------------------+------------+----------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(transactions):
	# Write code here
	window_spec = W.partitionBy("customer_id").orderBy("date")

  transactions= transactions.withColumn("previous_product",
                            F.lag(transactions["product_id"]).over(window_spec))

  transactions =  transactions.withColumn(
                                "previous_product",
                                F.when(F.col("previous_product").isNull(),"None",)\
                                .otherwise(F.col("previous_product"),
                            ))

  final_df= transactions.withColumn("date_and_product",
                                     F.concat_ws(" ",
                                                 F.col("date"),
                                                 F.col("previous_product"))
                                     )
  return final_df
