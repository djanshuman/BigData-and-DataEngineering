/* 
Retail Stores

In a retail store, we often keep track of the customer transactions to maintain inventory, sales and customer data. The data often contains customer information, purchase details and transaction date.

transactions has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| customer_id | string    |
| product_id  | string    |
| quantity    | integer   |
| date        | string    |
+-------------+-----------+


The date field contains the date of transaction in the format 'YYYY-MM-DD'. The customer_id field contains the unique ID of a customer, the product_id contains the unique ID of a product and quantity contains the quantity of the product purchased in the transaction.

Write a function that computes the following: for each row, we add a new column previous_product that contains the product_id of the previous transaction made by the same customer, in chronological order. If there's no previous transaction for a customer, the previous_product column should contain None. Also, the date and previous_product columns should be concatenated into a new column date_and_product, separated by a space.


The output DataFrame should have the following schema:

+------------------+-----------+
| Column Name      | Data Type |
+------------------+-----------+
| customer_id      | string    |
| product_id       | string    |
| quantity         | integer   |
| date             | string    |
| previous_product | string    |
| date_and_product | string    |
+------------------+-----------+
 
Example
transactions
+-------------+------------+----------+------------+
| customer_id | product_id | quantity | date       |
+-------------+------------+----------+------------+
| CUST1       | PROD1      | 2        | 2023-01-01 |
| CUST1       | PROD2      | 1        | 2023-01-05 |
| CUST2       | PROD3      | 5        | 2023-02-03 |
| CUST3       | PROD1      | 4        | 2023-02-07 |
| CUST1       | PROD3      | 3        | 2023-02-10 |
+-------------+------------+----------+------------+

Expected
+-------------+------------+------------------+------------------+------------+----------+
| customer_id | date       | date_and_product | previous_product | product_id | quantity |
+-------------+------------+------------------+------------------+------------+----------+
| CUST1       | 2023-01-01 | 2023-01-01 None  | None             | PROD1      | 2        |
| CUST1       | 2023-01-05 | 2023-01-05 PROD1 | PROD1            | PROD2      | 1        |
| CUST1       | 2023-02-10 | 2023-02-10 PROD2 | PROD2            | PROD3      | 3        |
| CUST2       | 2023-02-03 | 2023-02-03 None  | None             | PROD3      | 5        |
| CUST3       | 2023-02-07 | 2023-02-07 None  | None             | PROD1      | 4        |
+-------------+------------+------------------+------------------+------------+----------+
*/


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(transactions: DataFrame): DataFrame = {
	val WindowSpec= Window.partitionBy("customer_id").orderBy("date")

    val newDF= transactions
                .withColumn("previous_product",lag("product_id",1).over(WindowSpec))
                .withColumn("previous_product",when(col("previous_product").isNull,"None").otherwise(col("previous_product")))
    
    val concatDF = newDF
            .withColumn("date_and_product",concat_ws(" ",col("date"),col("previous_product")))
    concatDF
}
