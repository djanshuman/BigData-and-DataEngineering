'''
Aerospace Equipment
 

In this problem, you are given two DataFrames: aerospace_df and company_df.
 
aerospace_df contains information about various aerospace equipment. It has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|     id      |  string   |
|    name     |  string   |
|    type     |  string   |
|   status    |  string   |
| company_id  |  string   |
+-------------+-----------+
 

company_df contains information about different companies in the aerospace industry. It has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|     id      |  string   |
|    name     |  string   |
|   country   |  string   |
+-------------+-----------+
 

The goal is to join these two DataFrames so that they have the following schema:

+------------------+-----------+
|   Column Name    | Data Type |
+------------------+-----------+
|        id        |  string   |
|  equipment_name  |  string   |
|  equipment_type  |  string   |
| equipment_status |  string   |
|   company_name   |  string   |
|     country      |  string   |
|   status_label   |  string   |
+------------------+-----------+

 
The status_label column in the output is derived from the status column of the aerospace_df DataFrame and the country column of company_df. If the status is "active" and the country is "USA", the status_label should be "Domestic Active". If the status is "active" and the country is not "USA", the status_label should be "Foreign Active". If the status is not "active", regardless of the country, the status_label should be "Inactive".
 
 
 

Example

aerospace_df
+----+-----------+-----------+----------+------------+
| id |   name    |   type    |  status  | company_id |
+----+-----------+-----------+----------+------------+
| A1 | Falcon 9  |  Rocket   |  active  |     C1     |
| A2 | Starship  |  Rocket   |  active  |     C1     |
| A3 |  Hubble   | Telescope |  active  |     C2     |
| A4 |  Galileo  | Satellite | inactive |     C3     |
| A5 | Voyager 1 |   Probe   |  active  |     C3     |
+----+-----------+-----------+----------+------------+

company_df
+----+-----------------------+---------+
| id |         name          | country |
+----+-----------------------+---------+
| C1 |        SpaceX         |   USA   |
| C2 |         NASA          |   USA   |
| C3 | European Space Agency | Europe  |
+----+-----------------------+---------+

Expected
+-----------------------+---------+----------------+------------------+----------------+----+-----------------+
|     company_name      | country | equipment_name | equipment_status | equipment_type | id |  status_label   |
+-----------------------+---------+----------------+------------------+----------------+----+-----------------+
| European Space Agency | Europe  |    Galileo     |     inactive     |   Satellite    | A4 |    Inactive     |
| European Space Agency | Europe  |   Voyager 1    |      active      |     Probe      | A5 | Foreign Active  |
|         NASA          |   USA   |     Hubble     |      active      |   Telescope    | A3 | Domestic Active |
|        SpaceX         |   USA   |    Falcon 9    |      active      |     Rocket     | A1 | Domestic Active |
|        SpaceX         |   USA   |    Starship    |      active      |     Rocket     | A2 | Domestic Active |
+-----------------------+---------+----------------+------------------+----------------+----+-----------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(aerospace_df, company_df):
	# Write code here

    joinedDF = aerospace_df.join(company_df,
                                 aerospace_df.company_id == company_df.id,"inner")

    condition = F.when( 
                (joinedDF.status == 'active') & (joinedDF.country == "USA") ,"Domestic Active")\
                    .when(
                        (joinedDF.status == 'active') & (joinedDF.country != "USA") ,"Foreign Active"
                    ).otherwise("Inactive")

    #return joinedDF 
    enrich_df= joinedDF.withColumn("status_label",condition)
    
    final_df = enrich_df.select(
                    company_df["name"].alias("company_name"),
                    "country",
                    aerospace_df["name"].alias("equipment_name"),
                    aerospace_df["status"].alias("equipment_status"),
                    aerospace_df["type"].alias("equipment_type"),
                    aerospace_df["id"].alias("id"),
                    "status_label"
    )

    return final_df
