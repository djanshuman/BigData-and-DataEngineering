'''
Government Budgeting
 

You are a Data Scientist working for the federal government. Your task involves analyzing budget and spending data across various departments. You have been given two DataFrames that represent these data sets.
 
The first, budget_df, consists of the following columns:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| Department  |  String   |
|    Year     |  Integer  |
|   Budget    |  Double   |
+-------------+-----------+

 
The Department field is the name of the federal department, Year is the fiscal year, and Budget is the total budget allocated to that department in that year, in millions of dollars.
 
 
The second, spending_df, has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| Department  |  String   |
|    Year     |  Integer  |
|  Spending   |  Double   |
+-------------+-----------+

 
The Department field is the name of the federal department, Year is the fiscal year, and Spending is the total amount of money spent by that department in that year, in millions of dollars.
 
Write a function that calculates the variance in the budget and spending for each department over the years. The output should have the following schema:
 
+-------------------+-----------+
|    Column Name    | Data Type |
+-------------------+-----------+
|    Department     |  String   |
|  Budget_Variance  |  Integer  |
| Spending_Variance |  Integer  |
+-------------------+-----------+
 
 
 

Example

budget_df
+------------+------+--------+
| Department | Year | Budget |
+------------+------+--------+
|   Health   | 2019 | 750.0  |
| Education  | 2019 | 500.0  |
|   Health   | 2020 | 800.0  |
| Education  | 2020 | 550.0  |
+------------+------+--------+

spending_df
+------------+------+----------+
| Department | Year | Spending |
+------------+------+----------+
|   Health   | 2019 |  700.0   |
| Education  | 2019 |  450.0   |
|   Health   | 2020 |  780.0   |
| Education  | 2020 |  540.0   |
+------------+------+----------+

Expected
+-----------------+------------+-------------------+
| Budget_Variance | Department | Spending_Variance |
+-----------------+------------+-------------------+
|      1250       | Education  |       4050        |
|      1250       |   Health   |       3200        |
+-----------------+------------+-------------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(budget_df, spending_df):
	# Write code here

    # budget variance

    window_spec = W.partitionBy("Department")

    budget_df_var = budget_df.withColumn("Budget_Variance",
                            F.variance("Budget").over(window_spec).cast("integer")
                            ).drop("Budget","Year")\
                        .dropDuplicates(["Department"])
    
    spending_df_var = spending_df.withColumn("Spending_Variance",
                            F.variance("Spending").over(window_spec).cast("integer")
                            ).drop("Spending","Year")\
                        .dropDuplicates(["Department"])
    
    
    agg_dedup_df = budget_df_var.join(spending_df_var,"Department","inner")
    return agg_dedup_df


    
