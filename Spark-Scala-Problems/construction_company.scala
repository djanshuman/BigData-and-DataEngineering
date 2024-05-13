/* 
Construction Company
 
You are working as a data analyst for a large construction company. Your task is to analyze the data from three different DataFrames related to the company's construction projects, employees, and equipment.
 
The three DataFrames have the following schemas:
 
projects:
+--------------+-----------+
| Column Name  | Data Type |
+--------------+-----------+
| project_id   | int       |
| project_name | string    |
| start_date   | date      |
| end_date     | date      |
| budget       | int       |
+--------------+-----------+
 
employees:
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| employee_id | int       |
| first_name  | string    |
| last_name   | string    |
| role        | string    |
| project_id  | int       |
+-------------+-----------+
 
equipment:
+----------------+-----------+
| Column Name    | Data Type |
+----------------+-----------+
| equipment_id   | int       |
| equipment_name | string    |
| project_id     | int       |
| cost           | int       |
+----------------+-----------+
 
Write a function that aggregates the following information for each project: the project's duration in days, the total number of employees working on the project, the number of unique roles in the project, and the total cost of equipment for the project.
 
Output Schema:
+----------------------+-----------+
| Column Name          | Data Type |
+----------------------+-----------+
| project_id           | int       |
| project_name         | string    |
| start_date           | date      |
| end_date             | date      |
| duration_days        | int       |
| total_employees      | int       |
| unique_roles         | int       |
| total_equipment_cost | int       |
+----------------------+-----------+
 
 
 
Example (Slide panel to right to view tables full)
 
projects
+------------+--------------+------------+------------+----------+
| project_id | project_name | start_date | end_date   | budget   |
+------------+--------------+------------+------------+----------+
| 1          | Skyscraper   | 2022-01-01 | 2022-12-31 | 15000000 |
| 2          | Bridge       | 2022-03-01 | 2022-08-31 | 5000000  |
| 3          | Tunnel       | 2022-06-01 | 2023-01-31 | 10000000 |
+------------+--------------+------------+------------+----------+

employees
+-------------+------------+-----------+-----------------+------------+
| employee_id | first_name | last_name | role            | project_id |
+-------------+------------+-----------+-----------------+------------+
| 1           | John       | Doe       | Engineer        | 1          |
| 2           | Jane       | Smith     | Architect       | 1          |
| 3           | Jim        | Brown     | Project Manager | 1          |
| 4           | Emily      | Davis     | Engineer        | 2          |
| 5           | Alan       | Johnson   | Architect       | 2          |
+-------------+------------+-----------+-----------------+------------+

equipment
+--------------+----------------+------------+-------+
| equipment_id | equipment_name | project_id | cost  |
+--------------+----------------+------------+-------+
| 1            | Crane          | 1          | 25000 |
| 2            | Excavator      | 1          | 15000 |
| 3            | Bulldozer      | 2          | 20000 |
| 4            | Loader         | 2          | 10000 |
| 5            | Crane          | 3          | 25000 |
+--------------+----------------+------------+-------+

Output
+---------------+------------+------------+--------------+------------+-----------------+----------------------+--------------+
| duration_days | end_date   | project_id | project_name | start_date | total_employees | total_equipment_cost | unique_roles |
+---------------+------------+------------+--------------+------------+-----------------+----------------------+--------------+
| 183           | 2022-08-31 | 2          | Bridge       | 2022-03-01 | 2.0             | 30000                | 2.0          |
| 244           | 2023-01-31 | 3          | Tunnel       | 2022-06-01 | NaN             | 25000                | NaN          |
| 364           | 2022-12-31 | 1          | Skyscraper   | 2022-01-01 | 3.0             | 40000                | 3.0          |
+---------------+------------+------------+--------------+------------+-----------------+----------------------+--------------+
*/


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(projects: DataFrame, employees: DataFrame, equipment: DataFrame): DataFrame = {
    val projectDuration = projects
                    .withColumn("duration_days", 
                                datediff(col("end_date"),col("start_date")))
    
    val employeeDF = employees
                .groupBy("project_id")
                .agg(
                    count("employee_id").alias("total_employees"),
                    countDistinct("role").alias("unique_roles")                
                )
    val equipDF = equipment
                    .groupBy("project_id")
                    .agg(
                        sum("cost").alias("total_equipment_cost")
                    )
    val resultDF =  projectDuration
                        .join(employeeDF,Seq("project_id"),"left")
                        .join(equipDF,Seq("project_id"),"left")
                        .select("project_id",
                          "project_name",
                          "start_date",
                          "end_date",
                          "duration_days",
                          "total_employees",
                          "unique_roles",
                          "total_equipment_cost"
                        )
                        resultDF

}

