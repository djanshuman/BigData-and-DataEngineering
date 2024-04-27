/*
Social Media PII


A social media company stores user information in a table, including their email addresses and phone numbers. Write a function that returns the processed user information. The function should perform the following string manipulations:


Extract the domain name from the email addresses.
Anonymize the phone numbers by replacing the first six digits with asterisks.


input_df has the following schema:
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| user_id     | Integer   |
| email       | String    |
| phone       | Integer   |
+-------------+-----------+


The output DataFrame should have the following schema:
+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| user_id      | Integer   |
| email_domain | String    |
| anon_phone   | String    |
+--------------+-----------+


Example


input_df
+---------+-------------------+------------+
| user_id | email             | phone      |
+---------+-------------------+------------+
| 1       | alice@example.com | 5551234567 |
| 2       | bob@domain.net    | 5559876543 |
| 3       | carol@email.org   | 5551239876 |
| 4       | dave@site.com     | 5554567890 |
| 5       | eve@platform.io   | 5559871234 |
+---------+-------------------+------------+

Output
+------------+--------------+---------+
| anon_phone | email_domain | user_id |
+------------+--------------+---------+
| ******1234 | platform.io  | 5       |
| ******4567 | example.com  | 1       |
| ******6543 | domain.net   | 2       |
| ******7890 | site.com     | 4       |
| ******9876 | email.org    | 3       |
+------------+--------------+---------+
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(input_df: DataFrame): DataFrame = {
    // extract domain name 

    val email_domain= regexp_extract(col("email"),"@(.+)",1)
    val df1= input_df.withColumn("email_domain",email_domain)
    //anonymise phone number
    val anon_phone = regexp_replace(col("phone"),"^\\d{6}","******")
    val df2 = df1.withColumn("anon_phone",anon_phone)
    
    val final_df = df2.select("anon_phone","email_domain","user_id")
    final_df
    
}
