/*
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
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(df_transactions: DataFrame, df_clients: DataFrame): DataFrame = {
        
    val transactionWindow = Window.partitionBy("TransactionID").orderBy("TransactionID")
        val clientWindow = Window.partitionBy("ClientID").orderBy("ClientID")

        //remove duplicates and keep first transaction and client details

        val transDeDup = df_transactions
                        .withColumn("rn", row_number().over(transactionWindow))
                        .filter("rn == 1")
                        .drop("rn")

        val clientDedup = df_clients
                        .withColumn("rn", row_number().over(clientWindow))
                        .filter("rn == 1")
                        .drop("rn")
        
        //remove invalid trans_id and client_id
        val transDF= transDeDup
                    .filter(col("TransactionID") > 0)
                    .filter(col("Date").rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"))
        
        val clientDF= clientDedup.filter(col("clientid") > 0)

        val joinedDF= transDF
                    .join(clientDF,"ClientID")
                    .select(
                        "amount",
                        "ClientID",
                        "clientname",
                        "date",
                        "industry",
                        "transactionid"
                    )
                    
        joinedDF
}
