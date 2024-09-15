'''
Running Payroll



You work in the payroll department and are asked to process the payroll data of a company. You are given two DataFrames: employees and payroll. employees contains employee information, and payroll contains the payroll data.



Write a function that returns the calculated pay for each employee.



employees

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| employee_id | Integer   |
| name        | String    |
| age         | Integer   |
| position    | String    |
+-------------+-----------+


payroll

+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| employee_id  | Integer   |
| hours_worked | Float     |
| hourly_rate  | Float     |
+--------------+-----------+


Output DataFrame:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| employee_id | Integer   |
| name        | String    |
| position    | String    |
| pay         | Float     |
+-------------+-----------+


The pay should be calculated as follows:

If an employee works less than or equal to 40 hours, their pay is equal to the product of the hours worked and the hourly rate.
If an employee works more than 40 hours, they are paid the regular hourly rate for the first 40 hours and 1.5 times the hourly rate for any hours worked above 40.


Example




employees
+-------------+-------+-----+-------------------+
| employee_id | name  | age | position          |
+-------------+-------+-----+-------------------+
| 1           | Alice | 25  | Software Engineer |
| 2           | Bob   | 30  | Data Analyst      |
| 3           | Carol | 28  | Product Manager   |
| 4           | Dave  | 24  | Software Engineer |
+-------------+-------+-----+-------------------+

payroll
+-------------+--------------+-------------+
| employee_id | hours_worked | hourly_rate |
+-------------+--------------+-------------+
| 1           | 45.0         | 30.0        |
| 2           | 38.0         | 25.0        |
| 3           | 41.5         | 35.0        |
| 4           | 40.0         | 28.0        |
+-------------+--------------+-------------+

Output
+-------------+-------+---------+-------------------+
| employee_id | name  | pay     | position          |
+-------------+-------+---------+-------------------+
| 1           | Alice | 1425.00 | Software Engineer |
| 2           | Bob   | 950.00  | Data Analyst      |
| 3           | Carol | 1478.75 | Product Manager   |
| 4           | Dave  | 1120.00 | Software Engineer |
+-------------+-------+---------+-------------------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(employees, payroll):
	# Write code here
	empDF= employees.join(payroll,on="employee_id",how="inner")
  pay_total = F.when(
           empDF.hours_worked <= 40,
           empDF.hours_worked 
           * empDF.hourly_rate
    ).otherwise(
        (40 * empDF.hourly_rate)
        + ( 
            (empDF.hours_worked - 40) 
            * empDF.hourly_rate
            * 1.5 
            )
         )    
  finalDF= empDF.withColumn("pay",pay_total).drop("age","hourly_rate","hours_worked")
  return finalDF


