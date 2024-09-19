'''
Background Checks
 

A background check company has a database of the checks they have performed. They want to process this data for further analysis. You are given a DataFrame background_checks that stores these checks. The background_checks DataFrame has the following schema:

+--------------------+-----------+
|    Column Name     | Data Type |
+--------------------+-----------+
|      check_id      |  Integer  |
|     full_name      |  String   |
|        dob         |   Date    |
|  criminal_record   |  String   |
| employment_history |  String   |
| education_history  |  String   |
|      address       |  String   |
+--------------------+-----------+

 
The criminal_record, employment_history, education_history and address fields are comma-separated varchar fields that hold multiple pieces of information.
 
 
Output Schema:

+--------------------+-----------+
|    Column Name     | Data Type |
+--------------------+-----------+
|      check_id      |  Integer  |
|     full_name      |  String   |
|        dob         |   Date    |
|    crime_count     |  Integer  |
|     jobs_count     |  Integer  |
|   degrees_count    |  Integer  |
| places_lived_count |  Integer  |
+--------------------+-----------+
 

In the output:
 
The crime_count column is the count of crimes in the criminal_record column.
The jobs_count column is the count of jobs in the employment_history column.
The degrees_count column is the count of degrees in the education_history column.
The places_lived_count column is the count of addresses in the address column.
 
Write a function that transforms the background_checks DataFrame to have the desired output schema.
 

Example

background_checks
+----------+-------------+------------+--------------------------+----------------------------+---------------------------------+-------------------------+
| check_id |  full_name  |    dob     |     criminal_record      |     employment_history     |        education_history        |         address         |
+----------+-------------+------------+--------------------------+----------------------------+---------------------------------+-------------------------+
|    1     |  John Doe   | 1980-05-05 |      Theft,Assault       |     ABC Corp,XYZ Corp      | B.A. in English,M.A. in English | 123 Main St,456 Pine St |
|    2     | Jane Smith  | 1982-07-12 |           DUI            | DEF Corp,GHI Corp,JKL Corp |        B.Sc. in Physics         |       789 Oak St        |
|    3     | Bob Johnson | 1975-10-20 | Theft,Fraud,Embezzlement |          MNO Corp          | B.A. in History,M.A. in History | 321 Cedar St,654 Elm St |
+----------+-------------+------------+--------------------------+----------------------------+---------------------------------+-------------------------+

Expected
+----------+-------------+---------------+------------+-------------+------------+--------------------+
| check_id | crime_count | degrees_count |    dob     |  full_name  | jobs_count | places_lived_count |
+----------+-------------+---------------+------------+-------------+------------+--------------------+
|    1     |      2      |       2       | 1980-05-05 |  John Doe   |     2      |         2          |
|    2     |      1      |       1       | 1982-07-12 | Jane Smith  |     3      |         1          |
|    3     |      3      |       2       | 1975-10-20 | Bob Johnson |     1      |         2          |
+----------+-------------+---------------+------------+-------------+------------+--------------------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(background_checks):
	# Write code here
    myudf = F.udf(
        lambda x: len((x.split(","))) if x else 0,
        pyspark.sql.types.IntegerType(),
    )

    background_checks = background_checks.withColumn("crime_count",
                                           myudf(F.col("criminal_record")))
    
    background_checks = background_checks.withColumn("jobs_count",
                                           myudf(F.col("employment_history")))

    background_checks = background_checks.withColumn("degrees_count",
                                           myudf(F.col("education_history")))

    background_checks = background_checks.withColumn("places_lived_count",
                                           myudf(F.col("address")))

    finalDF = background_checks.select("check_id",
                           "crime_count",
                            "degrees_count",
                            "dob",
                            "full_name",
                            "jobs_count",
                            "places_lived_count"
                           )
    return finalDF
    
    
