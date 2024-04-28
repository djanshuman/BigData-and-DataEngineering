/*
Food and Beverage Sales

You are in the Food and Beverage Industry and are given 3 DataFrames containing information about various products. Create a function that processes and combines the data from these DataFrames.

The input DataFrames have the following schemas:
 
products
+------------+---------+
| Column     | Type    |
+------------+---------+
| product_id | integer |
| name       | string  |
| category   | string  |
+------------+---------+

sales
+------------+---------+
| Column     | Type    |
+------------+---------+
| sale_id    | integer |
| product_id | integer |
| quantity   | integer |
| revenue    | integer |
+------------+---------+

inventory
+------------+---------+
| Column     | Type    |
+------------+---------+
| product_id | integer |
| stock      | integer |
| warehouse  | string  |
+------------+---------+

The function should perform the following steps:
For each product, find the total sales quantity and revenue.
For each product, find the total stock available across all warehouses.
Combine the information from steps 1 and 2, and include the product name and category.
Replace any null values in the total sales quantity, revenue, and total stock columns with a 0.
 
The output DataFrame should have the following schema:
+----------------+---------+
| Column         | Type    |
+----------------+---------+
| product_id     | integer |
| name           | string  |
| category       | string  |
| total_quantity | integer |
| total_revenue  | integer |
| total_stock    | integer |
+----------------+---------+

 
 
Example

products
+------------+--------------------+-----------+
| product_id | name               | category  |
+------------+--------------------+-----------+
| 1          | Apple Juice        | Beverages |
| 2          | Orange Juice       | Beverages |
| 3          | Chocolate Bar      | Snacks    |
| 4          | Potato Chips       | Snacks    |
| 5          | Fresh Strawberries | Fruits    |
+------------+--------------------+-----------+

sales
+---------+------------+----------+---------+
| sale_id | product_id | quantity | revenue |
+---------+------------+----------+---------+
| 1       | 1          | 10       | 20.0    |
| 2       | 1          | 5        | 10.0    |
| 3       | 2          | 8        | 16.0    |
| 4       | 3          | 2        | 4.0     |
| 5       | 4          | 15       | 30.0    |
+---------+------------+----------+---------+

inventory
+------------+-------+------------+
| product_id | stock | warehouse  |
+------------+-------+------------+
| 1          | 50    | Warehouse1 |
| 2          | 40    | Warehouse1 |
| 3          | 30    | Warehouse1 |
| 4          | 20    | Warehouse1 |
| 5          | 10    | Warehouse1 |
+------------+-------+------------+

Output
+-----------+--------------------+------------+----------------+---------------+-------------+
| category  | name               | product_id | total_quantity | total_revenue | total_stock |
+-----------+--------------------+------------+----------------+---------------+-------------+
| Beverages | Apple Juice        | 1          | 15             | 30            | 50          |
| Beverages | Orange Juice       | 2          | 8              | 16            | 40          |
| Fruits    | Fresh Strawberries | 5          | 0              | 0             | 10          |
| Snacks    | Chocolate Bar      | 3          | 2              | 4             | 30          |
| Snacks    | Potato Chips       | 4          | 15             | 30            | 20          |
+-----------+--------------------+------------+----------------+---------------+-------------+
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(products: DataFrame, sales: DataFrame, inventory: DataFrame): DataFrame = {
    /* For each product, find the total sales quantity and revenue. */

    val salesDF = sales.groupBy("product_id")
                        .agg(
                            sum("quantity").alias("total_quantity"),
                            sum("revenue").alias("total_revenue")
                         )
    val warehouseDF= inventory.groupBy("product_id")
                    .agg(
                        sum("stock").alias("total_stock")
                        )
    val aggDF= products
                  .join(salesDF, Seq("product_id") ,"left")
                  .join(warehouseDF, Seq("product_id") ,"left")

    val result_filled = aggDF
    .withColumn("total_quantity", coalesce(col("total_quantity"), lit(0)))
    .withColumn("total_revenue", coalesce(col("total_revenue"), lit(0.0)))
    .withColumn("total_stock", coalesce(col("total_stock"), lit(0)))

    result_filled
}
