/*
Ecommerce Datetimes

You work for an E-Commerce giant that has provided you two separate DataFrames.


The first, df_orders, contains order information with the following schema:


+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|  order_id   |  string   |
| product_id  |  string   |
|   user_id   |  string   |
| order_date  |  string   |
+-------------+-----------+


The order_date column contains the date of order placement in 'MM/DD/YYYY' format.

The second, df_products, contains product details with the following schema:


+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
|  product_id  |  string   |
| product_name |  string   |
|   category   |  string   |
+--------------+-----------+

 

Write a function that combines the DataFrames and creates a column named is_weekend that should indicate whether the order was placed on a weekend (Saturday or Sunday). Please be aware that you might encounter incorrect date formats, which should be handled appropriately. Also note that an order might have multiple products, so the same order might appear multiple times in the output.

 


+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
|   user_id    |  string   |
| product_name |  string   |
|   category   |  string   |
|  order_date  |  string   |
|  is_weekend  |  boolean  |
+--------------+-----------+

 

 

Example

df_orders
+----------+------------+---------+------------+
| order_id | product_id | user_id | order_date |
+----------+------------+---------+------------+
|    1     |    P001    |  U001   | 02/25/2023 |
|    2     |    P002    |  U001   | 03/14/2023 |
|    3     |    P001    |  U002   | 03/16/2023 |
|    4     |    P003    |  U002   | 03/18/2023 |
|    5     |    P004    |  U003   | 04/01/2023 |
+----------+------------+---------+------------+

df_products
+------------+--------------+-------------+
| product_id | product_name |  category   |
+------------+--------------+-------------+
|    P001    |  Product 1   | Electronics |
|    P002    |  Product 2   |  Clothing   |
|    P003    |  Product 3   | Home Goods  |
|    P004    |  Product 4   |    Books    |
+------------+--------------+-------------+

Expected
+-------------+------------+------------+--------------+---------+
|  category   | is_weekend | order_date | product_name | user_id |
+-------------+------------+------------+--------------+---------+
|    Books    |     1      | 04/01/2023 |  Product 4   |  U003   |
|  Clothing   |     0      | 03/14/2023 |  Product 2   |  U001   |
| Electronics |     0      | 03/16/2023 |  Product 1   |  U002   |
| Electronics |     1      | 02/25/2023 |  Product 1   |  U001   |
| Home Goods  |     1      | 03/18/2023 |  Product 3   |  U002   |
+-------------+------------+------------+--------------+---------+

*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(df_orders: DataFrame, df_products: DataFrame): DataFrame = {

    //re-format date to correct format

    val df_orders_DF= df_orders
                    .withColumn("order_date",
                                to_date(unix_timestamp($"order_date","MM/dd/yyyy").cast("timestamp")))

    val df_orders_DF_weekday = df_orders_DF
                    .withColumn("dayofweek",dayofweek($"order_date"))
    
    val df_orders_week= df_orders_DF_weekday.
                        withColumn("is_weekend",
                        when($"dayofweek" === 1 || $"dayofweek" ===7, true).otherwise(false))

    val mergedDF = df_orders_week.join(df_products,"product_id")
                .withColumn("order_date",date_format($"order_date","MM/dd/yyyy"))
    
    val finalDF = mergedDF.select(
                    "category",
                    "is_weekend",
                    "order_date",
                    "product_name",
                    "user_id"
    ) 
    finalDF
}
