'''
Pharmaceutical Equipment
 
A pharmaceutical company tracks its equipment data in two different DataFrames. The first, df1, contains information about the equipment, such as equipment_id, equipment_name, and purchase_date. The second, df2, keeps track of the maintenance details for each equipment, which includes equipment_id, maintenance_date, and maintenance_cost.
 
Write a function that returns the following columns: equipment_id, equipment_name, purchase_date, latest_maintenance_date, and maintenance_cost_rank. The latest_maintenance_date is the most recent maintenance date for each equipment. The maintenance_cost_rank is a dense rank, where rank 1 represents the equipment with the highest maintenance cost and so on.
 
Ensure that the output only contains the equipment that has at least one maintenance record in df2.
 
Schemas
 
df1

+----------------+-----------+
|  Column Name   | Data Type |
+----------------+-----------+
|  equipment_id  |  string   |
| equipment_name |  string   |
| purchase_date  |   date    |
+----------------+-----------+
 

df2

+------------------+-----------+
|   Column Name    | Data Type |
+------------------+-----------+
|   equipment_id   |  string   |
| maintenance_date |   date    |
| maintenance_cost |  double   |
+------------------+-----------+
 

Output:

+-------------------------+-----------+
|       Column Name       | Data Type |
+-------------------------+-----------+
|      equipment_id       |  string   |
|     equipment_name      |  string   |
|      purchase_date      |   date    |
| latest_maintenance_date |   date    |
|  maintenance_cost_rank  |  integer  |
+-------------------------+-----------+
 

Example

df1
+--------------+----------------+---------------+
| equipment_id | equipment_name | purchase_date |
+--------------+----------------+---------------+
|    EQ001     |     Mixer      |  2020-01-01   |
|    EQ002     |   Centrifuge   |  2020-02-01   |
|    EQ003     |    Pipette     |  2020-03-01   |
+--------------+----------------+---------------+

df2
+--------------+------------------+------------------+
| equipment_id | maintenance_date | maintenance_cost |
+--------------+------------------+------------------+
|    EQ001     |    2021-06-01    |      500.0       |
|    EQ002     |    2021-07-01    |      400.0       |
|    EQ001     |    2021-07-02    |      600.0       |
+--------------+------------------+------------------+

Expected
+--------------+----------------+-------------------------+-----------------------+---------------+
| equipment_id | equipment_name | latest_maintenance_date | maintenance_cost_rank | purchase_date |
+--------------+----------------+-------------------------+-----------------------+---------------+
|    EQ001     |     Mixer      |       2021-07-02        |           1           |  2020-01-01   |
|    EQ002     |   Centrifuge   |       2021-07-01        |           1           |  2020-02-01   |
+--------------+----------------+-------------------------+-----------------------+---------------+
'''

# solution 1

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
	# Write code here

    window_spec = W.partitionBy("equipment_id").\
            orderBy(F.desc("maintenance_cost"),F.desc("maintenance_date"))

    enrich_df1 = df2.withColumn("rnk",
                            F.dense_rank().over(window_spec)
                              ).filter(F.col("rnk")==1)
    
    enrich_df2= df1.join(enrich_df1,"equipment_id","inner").\
                    select("equipment_id",\
                        "equipment_name",
                        enrich_df1["maintenance_date"].alias("latest_maintenance_date"),
                        enrich_df1["rnk"].alias("maintenance_cost_rank"),
                        "purchase_date")
    return enrich_df2


#solution 2 having separate window then matching dates to filter records

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
    # Join two dataframes
    df = df1.join(
        df2, on="equipment_id", how="inner"
    )

    # Generate Window for latest maintenance date
    window_latest_maintenance = W.partitionBy(
        "equipment_id"
    ).orderBy(F.desc("maintenance_date"))
    df = df.withColumn(
        "latest_maintenance_date",
        F.first("maintenance_date").over(
            window_latest_maintenance
        ),
    )

    # Generate Window for rank
    window_rank = W.partitionBy(
        "equipment_id"
    ).orderBy(F.desc("maintenance_cost"))
    df = df.withColumn(
        "maintenance_cost_rank",
        F.dense_rank().over(window_rank),
    )

    # Filter to keep only the records with the latest maintenance date
    df = df.filter(
        F.col("maintenance_date")
        == F.col("latest_maintenance_date")
    )

    # Select required columns and drop duplicates
    df = df.select(
        "equipment_id",
        "equipment_name",
        "purchase_date",
        "latest_maintenance_date",
        "maintenance_cost_rank",
    ).dropDuplicates()

    return df
