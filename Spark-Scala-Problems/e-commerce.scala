/*
E-Commerce Platform


You have been given two DataFrames related to an E-commerce platform. The first contains information about the products and their categories, while the second contains information about the orders placed for these products. Write a function that calculates the average price and the total number of orders for each product category.


Input DataFrames:

products_df
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| product_id  | Integer   |
| category    | String    |
| price       | Float     |
+-------------+-----------+


orders_df
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| order_id    | Integer   |
| product_id  | Integer   |
| quantity    | Integer   |
+-------------+-----------+


Output DataFrame:
+--------------------+-----------+
| Column Name        | Data Type |
+--------------------+-----------+
| category           | String    |
| avg_price          | Float     |
| total_orders_count | Integer   |
+--------------------+-----------+


Example


products_df
+------------+----------+-------+
| product_id | category | price |
+------------+----------+-------+
| 1          | Apparel  | 25.99 |
| 2          | Apparel  | 35.99 |
| 3          | Footwear | 50.00 |
| 4          | Footwear | 75.00 |
| 5          | Apparel  | 20.00 |
+------------+----------+-------+

orders_df
+----------+------------+----------+
| order_id | product_id | quantity |
+----------+------------+----------+
| 101      | 1          | 2        |
| 102      | 2          | 1        |
| 103      | 1          | 3        |
| 104      | 3          | 1        |
| 105      | 4          | 2        |
+----------+------------+----------+

Output
+-----------+----------+--------------------+
| avg_price | category | total_orders_count |
+-----------+----------+--------------------+
| 29.323333 | Apparel  | 3                  |
| 62.500000 | Footwear | 2                  |
+-----------+----------+--------------------+
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(products_df: DataFrame, orders_df: DataFrame): DataFrame = {
	val joinedDF= products_df.join(orders_df,"product_id")
    val total_order_df= joinedDF
                  .groupBy("category")
                  .agg(
                      count("order_id").alias("total_orders_count"))
    
    val avg_price_df=joinedDF
                  .groupBy("category")
                  .agg(
                      avg("price").alias("avg_price"))
    
    val resultDF = total_order_df.join(avg_price_df,"category")
    resultDF
    
}
