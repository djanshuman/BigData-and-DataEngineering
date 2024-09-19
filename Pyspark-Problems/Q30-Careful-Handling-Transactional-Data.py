'''
Careful Handling Transactional Data
 

An accounting firm handles transactional data from various clients. The firm receives the data in two separate DataFrames. The first, df_transactions, contains the transactional data. The second, df_clients, contains the clients information.
 
df_transactions has the following schema:
 

+---------------+-----------+
|  Column Name  | Data Type |
+---------------+-----------+
| TransactionID |  Integer  |
|   ClientID    |  Integer  |
|     Date      |  String   |
|    Amount     |   Float   |
+---------------+-----------+
 

df_clients has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|  ClientID   |  Integer  |
| ClientName  |  String   |
|  Industry   |  String   |
+-------------+-----------+
 

It is observed that there are a number of duplicated and incorrect primary keys in both DataFrames. Write a function that combines the DataFrames and has no duplicate or incorrect primary keys.
 
The output should have the following schema:

+---------------+-----------+
|  Column Name  | Data Type |
+---------------+-----------+
| TransactionID |  Integer  |
|   ClientID    |  Integer  |
|     Date      |  String   |
|    Amount     |  Integer  |
|  ClientName   |  String   |
|   Industry    |  String   |
+---------------+-----------+
 

Please note that:
 
A valid TransactionID in df_transactions is an integer value greater than 0 and each TransactionID should be unique. Any row with an invalid or duplicate TransactionID should be dropped.
 
A valid ClientID in both df_transactions and df_clients is an integer value greater than 0. Each ClientID in df_clients should be unique. Any row with an invalid or duplicate ClientID in df_clients should be dropped. Then the df_transactions DataFrame should only include rows with ClientIDs that exist in df_clients.
 
The Date column should be in the format 'yyyy-mm-dd'. Rows with invalid date format should be dropped.
 
  

Example

df_transactions
+---------------+----------+------------+--------+
| TransactionID | ClientID |    Date    | Amount |
+---------------+----------+------------+--------+
|       1       |    1     | 2023-07-01 | 100.0  |
|       2       |    1     | 2023-07-02 | 150.0  |
|       2       |    2     | 2023-07-01 | 200.0  |
|       3       |    3     | 2023-07-03 | 250.0  |
|      -4       |    4     | 2023-07-04 | 300.0  |
|       5       |    2     | 2023-25-01 | 350.0  |
|       6       |    1     | 2023-07-02 | 400.0  |
|       7       |    6     | 2023-07-01 | 450.0  |
|       8       |    7     | 2023-07-03 | 500.0  |
|       9       |    -8    | 2023-07-04 | 550.0  |
+---------------+----------+------------+--------+

df_clients
+----------+------------+-------------+
| ClientID | ClientName |  Industry   |
+----------+------------+-------------+
|    1     |  Client1   |    Tech     |
|    2     |  Client2   |   Finance   |
|    3     |  Client3   | Real Estate |
|    4     |  Client4   | Healthcare  |
|    5     |  Client5   |    Tech     |
|    1     |  Client6   |   Finance   |
|    6     |  Client7   | Real Estate |
|    -7    |  Client8   | Healthcare  |
|    8     |  Client9   |    Tech     |
|    2     |  Client10  |   Finance   |
+----------+------------+-------------+

Expected
+--------+----------+------------+------------+-------------+---------------+
| Amount | ClientID | ClientName |    Date    |  Industry   | TransactionID |
+--------+----------+------------+------------+-------------+---------------+
|  100   |    1     |  Client1   | 2023-07-01 |    Tech     |       1       |
|  150   |    1     |  Client1   | 2023-07-02 |    Tech     |       2       |
|  250   |    3     |  Client3   | 2023-07-03 | Real Estate |       3       |
|  350   |    2     |  Client2   | 2023-25-01 |   Finance   |       5       |
|  400   |    1     |  Client1   | 2023-07-02 |    Tech     |       6       |
|  450   |    6     |  Client7   | 2023-07-01 | Real Estate |       7       |
+--------+----------+------------+------------+-------------+---------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_transactions, df_clients):
	# Write code here

    window_spec_tx = W.partitionBy("TransactionID").orderBy("TransactionID")
    window_spec_cl = W.partitionBy("ClientID").orderBy("ClientID")


    # deduplication process
    df_transactions = df_transactions.withColumn("rn",
                                F.row_number().over(window_spec_tx)                      
                                ).filter(F.col("rn") ==1).drop("rn")

    df_clients = df_clients.withColumn("rn",
                                F.row_number().over(window_spec_cl)                      
                                ).filter(F.col("rn") ==1).drop("rn")

    # clean incorrect data

    df_transactions = df_transactions.filter(F.col("TransactionID") > 0 )
    df_clients = df_clients.filter(F.col("ClientID") > 0 )

    df_transactions = df_transactions.filter(
                    F.col("Date").rlike(
                        "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
                    )
    )

    final_df= df_transactions.join(df_clients,"ClientID","Inner")
    return final_df
    
