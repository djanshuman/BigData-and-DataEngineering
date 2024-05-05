/* 
Mountain Climbing
 
You are given two DataFrames: "mountain_info" and "mountain_climbers".
 
"mountain_info" contains information about different mountains including the name of the mountain, the height of the mountain, the country in which the mountain is located, and the range in which the mountain is located. The schema of "mountain_info" is as follows:
 
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| name        | string    |
| height      | integer   |
| country     | string    |
| range       | string    |
+-------------+-----------+
 
The "mountain_climbers" DataFrame contains information about climbers who have climbed different mountains. It includes the name of the climber, the name of the mountain they climbed, the date they climbed the mountain, and the time they took to climb the mountain. The schema of "mountain_climbers" is as follows:
 
+---------------+-----------+
| Column Name   | Data Type |
+---------------+-----------+
| climber_name  | string    |
| mountain_name | string    |
| climb_date    | date      |
| climb_time    | double    |
+---------------+-----------+
 
Write a function that returns the name of the mountain, the name of the climber who climbed the mountain last, and the date and time when they climbed it last and should only contain the mountains that have been climbed by at least one climber. The output schema is as follows:
 
+-------------------+-----------+
| Column Name       | Data Type |
+-------------------+-----------+
| mountain_name     | string    |
| last_climber_name | string    |
| last_climb_date   | date      |
| last_climb_time   | double    |
+-------------------+-----------+
 
Example
 
mountain_info
+-------------------+--------+----------+-------------+
| name              | height | country  | range       |
+-------------------+--------+----------+-------------+
| Mount Everest     | 8848   | Nepal    | Himalayas   |
| Mount Kilimanjaro | 5895   | Tanzania | Kilimanjaro |
| Mount Denali      | 6190   | USA      | Alaska      |
| Mount Fuji        | 3776   | Japan    | Fuji        |
| Mont Blanc        | 4808   | France   | Alps        |
+-------------------+--------+----------+-------------+

mountain_climbers
+--------------+-------------------+------------+------------+
| climber_name | mountain_name     | climb_date | climb_time |
+--------------+-------------------+------------+------------+
| John         | Mount Everest     | 2020-01-01 | 8.5        |
| Jane         | Mount Everest     | 2022-02-02 | 9.0        |
| Jim          | Mount Kilimanjaro | 2021-03-03 | 6.0        |
| Jess         | Mount Kilimanjaro | 2022-04-04 | 7.0        |
| Joe          | Mount Denali      | 2022-05-05 | 10.0       |
| Jill         | Mount Denali      | 2021-06-06 | 11.0       |
| Jack         | Mount Fuji        | 2022-07-07 | 4.0        |
| Jules        | Mount Fuji        | 2021-08-08 | 5.0        |
| Jean         | Mont Blanc        | 2020-09-09 | 12.0       |
| Josh         | Mont Blanc        | 2022-10-10 | 13.0       |
+--------------+-------------------+------------+------------+

Output
+------------+------------+--------------+-------------------+
| climb_date | climb_time | climber_name | mountain_name     |
+------------+------------+--------------+-------------------+
| 2022-02-02 | 9.0        | Jane         | Mount Everest     |
| 2022-04-04 | 7.0        | Jess         | Mount Kilimanjaro |
| 2022-05-05 | 10.0       | Joe          | Mount Denali      |
| 2022-07-07 | 4.0        | Jack         | Mount Fuji        |
| 2022-10-10 | 13.0       | Josh         | Mont Blanc        |
+------------+------------+--------------+-------------------+

*/


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(mountain_info: DataFrame, mountain_climbers: DataFrame): DataFrame = {
    val mountain_climbers_df= mountain_climbers.
                selectExpr("*",
                       "rank over(partition by mountain_name order by climb_date desc ,climb_time desc).as rank"
                      ).filter("rank == 1").drop("rank")

    val resultant_df= mountain_climbers_df.join(mountain_info,
                            mountain_info.mountain_name === mountain_climbers_df.mountain_name,
                            "inner")

    val resultDf= resultant_df.select(
                   "mountain_name",
                    "climber_name".as("last_climber_name"),
                    "climb_date".as("last_climb_date"),
                    "climb_time".as("last_climb_time")             
    )
    resultDf

}
