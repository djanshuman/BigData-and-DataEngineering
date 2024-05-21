/*
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
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(budget_df: DataFrame, spending_df: DataFrame): DataFrame = {
    val window= Window.partitionBy("Department")
    val budget_var= budget_df
                    .withColumn("Budget_Variance",
                               var_samp(col("Budget")).over(window)
                               )
    val spending_var =  spending_df
                        .withColumn("Spending_Variance",
                                   var_samp(col("Spending")).over(window).cast("Integer")
                                   )
    val budget_var_dedup = budget_var.dropDuplicates("department")
    val spending_var_dedup = spending_var.dropDuplicates("department")

    val joinedDF= budget_var_dedup
                    .join(spending_var_dedup,Seq("department"),"Inner")
                    .select("Budget_Variance","department","Spending_Variance")
    joinedDF
}
