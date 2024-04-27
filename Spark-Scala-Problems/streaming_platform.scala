Question:

You work for a video streaming platform and are given a DataFrame containing information about videos available on the platform. 
The DataFrame named input_df has the following schema:

+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| video_id     | Integer   |
| title        | String    |
| genre        | String    |
| release_year | Integer   |
| duration     | Integer   |
| view_count   | Integer   |
+--------------+-----------+


Your task is to write a function that takes in the input DataFrame and 
returns a DataFrame containing only the videos with more than 1,000,000 views and released in the last 5 years. 
The output DataFrame should have the same schema as the input DataFrame.

Solution:

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(input_df: DataFrame): DataFrame = {
    val current_year= Year.now().getValue
    val filtered_df= input_df.filter(
        $"view_count" > 1000000 && $"release_year" >= current_year - 5 
    )
    filtered_df
}
