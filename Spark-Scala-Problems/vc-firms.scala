/* 
VC Firms

You are given two DataFrames related to venture capital investments. Write a function that returns the total investment amount in each industry sector and is sorted by total_investment in descending order.
 
 companies
+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| company_id   | integer   |
| company_name | string    |
| industry     | string    |
+--------------+-----------+

investments
+---------------+-----------+
| Column Name   | Data Type |
+---------------+-----------+
| investment_id | integer   |
| company_id    | integer   |
| amount        | double    |
+---------------+-----------+

The output should have the following schema:
+------------------+-----------+
| Column Name      | Data Type |
+------------------+-----------+
| industry         | string    |
| total_investment | double    |
+------------------+-----------+
 
 

Example

companies
+------------+--------------------+------------------+
| company_id | company_name       | industry         |
+------------+--------------------+------------------+
| 1          | AlphaTech          | Technology       |
| 2          | BetaHealth         | Healthcare       |
| 3          | GammaEntertainment | Entertainment    |
| 4          | DeltaGreen         | Renewable Energy |
| 5          | EpsilonFinance     | Finance          |
+------------+--------------------+------------------+

investments
+---------------+------------+---------+
| investment_id | company_id | amount  |
+---------------+------------+---------+
| 1             | 1          | 5000000 |
| 2             | 2          | 3000000 |
| 3             | 3          | 1000000 |
| 4             | 4          | 4000000 |
| 5             | 5          | 2000000 |
+---------------+------------+---------+

Output
+------------------+------------------+
| industry         | total_investment |
+------------------+------------------+
| Entertainment    | 1000000          |
| Finance          | 2000000          |
| Healthcare       | 3000000          |
| Renewable Energy | 4000000          |
| Technology       | 5000000          |
+------------------+------------------+
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(companies: DataFrame, investments: DataFrame): DataFrame = {
        val joinedDF=companies.join(investments,"company_id")
        val agg_DF =joinedDF
            .groupBy("industry")
            .agg(
                sum("amount").alias("total_investment")
            )
        val sortedDF=agg_DF.sort(col("total_investment").desc)
        sortedDF
    
}
