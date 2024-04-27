/*
CRM SAAS Company

You are working on a Customer Relationship Management (CRM) software that manages information related to customers, orders, and products. Write a function that combines these DataFrames and creates a column named 'customer_name' that should be the concatenation of the first name and last name of the customer, separated by a space. 

Input DataFrames:
customers: Contains information about customers.
 
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| customer_id | Integer   |
| first_name  | String    |
| last_name   | String    |
| email       | String    |
+-------------+-----------+
 
orders: Contains information about orders placed by customers.
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| order_id    | Integer   |
| customer_id | Integer   |
| product_id  | Integer   |
| order_date  | Date      |
+-------------+-----------+
 
products: Contains information about products.
+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| product_id   | Integer   |
| product_name | String    |
| category     | String    |
+--------------+-----------+

Output DataFrame:
Your function should return the following schema:
 
+------------------+-----------+
| Column Name      | Data Type |
+------------------+-----------+
| order_id         | Integer   |
| customer_name    | String    |
| customer_email   | String    |
| product_name     | String    |
| product_category | String    |
| order_date       | Date      |
+------------------+-----------+
 
 
 
 Example

customers
+-------------+------------+-----------+----------------------+
| customer_id | first_name | last_name | email                |
+-------------+------------+-----------+----------------------+
| 1           | John       | Doe       | john.doe@email.com   |
| 2           | Jane       | Smith     | jane.smith@email.com |
+-------------+------------+-----------+----------------------+

orders
+----------+-------------+------------+------------+
| order_id | customer_id | product_id | order_date |
+----------+-------------+------------+------------+
| 1001     | 1           | 101        | 2023-01-10 |
| 1002     | 2           | 102        | 2023-01-11 |
+----------+-------------+------------+------------+

products
+------------+--------------+-----------+
| product_id | product_name | category  |
+------------+--------------+-----------+
| 101        | Product A    | Category1 |
| 102        | Product B    | Category2 |
+------------+--------------+-----------+

Output
+-----------+---------------+----------------------+------------+----------+--------------+
| category  | customer_name | email                | order_date | order_id | product_name |
+-----------+---------------+----------------------+------------+----------+--------------+
| Category1 | John Doe      | john.doe@email.com   | 2023-01-10 | 1001     | Product A    |
| Category2 | Jane Smith    | jane.smith@email.com | 2023-01-11 | 1002     | Product B    |
+-----------+---------------+----------------------+------------+----------+--------------+
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(customers: DataFrame, orders: DataFrame, products: DataFrame): DataFrame = {
    val joined_df= customers.join(orders,"customer_id").join(products,"product_id")
    val result_df= joined_df.select(
        $"order_id",
        concat_ws(" ",$"first_name",$"last_name").as("customer_name"),
        $"email",
        $"product_name",
        $"category",
        $"order_date"        
    )
    result_df
}
