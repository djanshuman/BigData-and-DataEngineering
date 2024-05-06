/* 
Factory Duplicates

In a manufacturing company, they collect data about their products and manufacturing processes. You are given two DataFrames. The first contains information about the products, and the second contains information about the manufacturing processes.

Write a function that removes duplicates from both DataFrames then combines them on their Product ID.

products_df
+-------------+-----------+------------------------------------+
| Column Name | Data Type | Description                        |
+-------------+-----------+------------------------------------+
| ProductID   | integer   | Unique identifier for each product |
| ProductName | string    | Name of the product                |
| Category    | string    | Category of the product            |
+-------------+-----------+------------------------------------+

manufacturing_processes_df
+-------------+-----------+--------------------------------------------------------+
| Column Name | Data Type | Description                                            |
+-------------+-----------+--------------------------------------------------------+
| ProcessID   | integer   | Unique identifier for each manufacturing process       |
| ProductID   | integer   | Identifier for the product associated with the process |
| ProcessName | string    | Name of the manufacturing process                      |
| Duration    | float     | Duration of the process in hours                       |
+-------------+-----------+--------------------------------------------------------+

Output DataFrame schema:
+-------------+-----------+--------------------------------------------------+
| Column Name | Data Type | Description                                      |
+-------------+-----------+--------------------------------------------------+
| ProductID   | integer   | Unique identifier for each product               |
| ProductName | string    | Name of the product                              |
| Category    | string    | Category of the product                          |
| ProcessID   | integer   | Unique identifier for each manufacturing process |
| ProcessName | string    | Name of the manufacturing process                |
| Duration    | float     | Duration of the process in hours                 |
+-------------+-----------+--------------------------------------------------+
 
 

Example

products_df
+-----------+-------------+----------+
| ProductID | ProductName | Category |
+-----------+-------------+----------+
| 1         | Widget A    | Type1    |
| 2         | Widget B    | Type1    |
| 3         | Widget C    | Type2    |
| 4         | Widget D    | Type2    |
| 1         | Widget A    | Type1    |
+-----------+-------------+----------+

manufacturing_processes_df
+-----------+-----------+-------------+----------+
| ProcessID | ProductID | ProcessName | Duration |
+-----------+-----------+-------------+----------+
| 1001      | 1         | Cutting     | 1.5      |
| 1002      | 2         | Cutting     | 1.6      |
| 1003      | 3         | Cutting     | 1.8      |
| 1004      | 4         | Cutting     | 1.5      |
| 1005      | 1         | Shaping     | 2.0      |
+-----------+-----------+-------------+----------+

Output
+----------+----------+-----------+-------------+-----------+-------------+
| Category | Duration | ProcessID | ProcessName | ProductID | ProductName |
+----------+----------+-----------+-------------+-----------+-------------+
| Type1    | 1.5      | 1001      | Cutting     | 1         | Widget A    |
| Type1    | 1.6      | 1002      | Cutting     | 2         | Widget B    |
| Type1    | 2.0      | 1005      | Shaping     | 1         | Widget A    |
| Type2    | 1.5      | 1004      | Cutting     | 4         | Widget D    |
| Type2    | 1.8      | 1003      | Cutting     | 3         | Widget C    |
+----------+----------+-----------+-------------+-----------+-------------+
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(products_df: DataFrame, manufacturing_processes_df: DataFrame): DataFrame = {

    val products_df_Dedup=products_df.dropDuplicates("ProductID")
    val manufacturing_processes__Dedup=manufacturing_processes_df.dropDuplicates("ProcessID","ProductID")
    
    val resultDF=products_df_Dedup
    .join(manufacturing_processes__Dedup,"ProductID")
    resultDF
    //manufacturing_processes__Dedup
}
