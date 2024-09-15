'''
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
'''


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(input_df):
	# Write code here
	email_domain= F.regexp_extract(
        F.col("email"), r"@(.+)", 1
            )
                    
  input_df = input_df.withColumn("email_domain",email_domain)

  anon_phone = F.regexp_replace(
        F.col("phone"), r"^\d{6}", "******"
        )
  input_df= input_df.withColumn("anon_phone",anon_phone)
  output_df = input_df.select(
        "user_id", "email_domain", "anon_phone"
        )

  return output_df
