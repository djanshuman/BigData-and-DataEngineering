/*
Manufacturing Plant

You are given two DataFrames related to a manufacturing company. The first, products, contains information about the products manufactured by the company, and the second, sales, contains information about the sales of these products.
Write a function that returns the top 3 selling products in each product category based on the revenue generated, without any gaps in the ranking sequence.

The input DataFrames have the following schemas:
products:
 
+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| product_id   | integer   |
| category     | string    |
| product_name | string    |
+--------------+-----------+

sales:
 
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| sale_id     | integer   |
| product_id  | integer   |
| quantity    | integer   |
| revenue     | double    |
+-------------+-----------+

The output DataFrame should have the following schema:
+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| category     | string    |
| product_name | string    |
| revenue      | integer   |
| rank         | integer   |
+--------------+-----------+
 
 
 
 Example
 
products
+------------+----------+--------------+
| product_id | category | product_name |
+------------+----------+--------------+
| 1          | A        | Product1     |
| 2          | A        | Product2     |
| 3          | A        | Product3     |
| 4          | B        | Product4     |
| 5          | B        | Product5     |
| 6          | B        | Product6     |
| 7          | C        | Product7     |
| 8          | C        | Product8     |
| 9          | C        | Product9     |
+------------+----------+--------------+

sales
+---------+------------+----------+---------+
| sale_id | product_id | quantity | revenue |
+---------+------------+----------+---------+
| 1       | 1          | 10       | 100.0   |
| 2       | 2          | 8        | 120.0   |
| 3       | 3          | 12       | 180.0   |
| 4       | 4          | 5        | 50.0    |
| 5       | 5          | 3        | 30.0    |
| 6       | 6          | 7        | 70.0    |
| 7       | 7          | 15       | 150.0   |
| 8       | 8          | 10       | 100.0   |
| 9       | 9          | 8        | 80.0    |
+---------+------------+----------+---------+

Output
+----------+--------------+------+---------+
| category | product_name | rank | revenue |
+----------+--------------+------+---------+
| A        | Product1     | 3    | 100     |
| A        | Product2     | 2    | 120     |
| A        | Product3     | 1    | 180     |
| B        | Product4     | 2    | 50      |
| B        | Product5     | 3    | 30      |
| B        | Product6     | 1    | 70      |
| C        | Product7     | 1    | 150     |
| C        | Product8     | 2    | 100     |
| C        | Product9     | 3    | 80      |
+----------+--------------+------+---------+
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(products: DataFrame, sales: DataFrame): DataFrame = {
    val sales_revenue_df=sales
                .groupBy("product_id")
                .agg(sum("revenue").alias("revenue"))
    
    val joined_df= sales_revenue_df.join(products,"product_id")
    val window_spec= Window.partitionBy("category").orderBy(desc("revenue"))

    val final_revenue_df= joined_df.select(
        $"category",
        $"product_name",
        $"revenue",
        dense_rank().over(window_spec).as("rank")
    )
    
    val top_products= final_revenue_df.filter($"rank" <= 3)
    top_products

}
