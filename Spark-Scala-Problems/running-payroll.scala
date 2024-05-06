/* 
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
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(employees: DataFrame, payroll: DataFrame): DataFrame = {
    val joinedDF=employees
            .join(payroll,employees("employee_id") === payroll("employee_id")
            ,"inner")
    val pay_val = when(
            joinedDF("hours_worked") <= 40,
            joinedDF("hours_worked") * joinedDF("hourly_rate")
    ) 
    .otherwise(
        (lit(40) * joinedDF("hourly_rate")) + 
        ((joinedDF("hours_worked") - 40) * 
        (joinedDF("hourly_rate")*1.5))
    )
    val finalDF= joinedDF
    .withColumn("pay",pay_val).drop("age","hourly_rate","hours_worked")
    finalDF
        
}
