'''
Machine Learning Metrics
 
As an AI engineer at an innovative technology company, you are given two DataFrames about the various AI models that the company has developed over the years. 
 
The first DataFrame, df_models, contains information about the AI models developed by the company. The schema of df_models is as follows:
 
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|  Model_ID   |  String   |
| Model_Name  |  String   |
| Model_Type  |  String   |
|  Accuracy   |   Float   |
+-------------+-----------+
 
The second DataFrame, df_usage, contains information about the usage of these AI models. The schema of df_usage is as follows:
 
+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|  Model_ID   |  String   |
|    Date     |   Date    |
|    Uses     |  Integer  |
+-------------+-----------+
 
Write a function that merges the information from both DataFrames based on Model_ID. In addition, it should compute the total number of uses for each model over time and the average accuracy of each model type.
 
The output DataFrame should have the following schema:
 
+------------------+-----------+
|   Column Name    | Data Type |
+------------------+-----------+
|     Model_ID     |  String   |
|    Model_Name    |  String   |
|    Model_Type    |  String   |
|     Accuracy     |   Float   |
|    Total_Uses    |  Integer  |
| Average_Accuracy |   Float   |
+------------------+-----------+
 
 
 
Example
 
df_models
+----------+------------+------------+----------+
| Model_ID | Model_Name | Model_Type | Accuracy |
+----------+------------+------------+----------+
|    M1    |   ModelA   |   Type1    |   0.85   |
|    M2    |   ModelB   |   Type2    |   0.78   |
|    M3    |   ModelC   |   Type1    |   0.88   |
|    M4    |   ModelD   |   Type3    |   0.92   |
+----------+------------+------------+----------+

df_usage
+----------+------------+------+
| Model_ID |    Date    | Uses |
+----------+------------+------+
|    M1    | 2023-01-01 | 100  |
|    M1    | 2023-01-02 | 120  |
|    M2    | 2023-01-01 | 200  |
|    M3    | 2023-01-01 | 150  |
|    M4    | 2023-01-02 | 130  |
+----------+------------+------+

Expected
+----------+------------------+----------+------------+------------+------------+
| Accuracy | Average_Accuracy | Model_ID | Model_Name | Model_Type | Total_Uses |
+----------+------------------+----------+------------+------------+------------+
|   0.78   |       0.78       |    M2    |   ModelB   |   Type2    |    200     |
|   0.85   |      0.865       |    M1    |   ModelA   |   Type1    |    220     |
|   0.88   |      0.865       |    M3    |   ModelC   |   Type1    |    150     |
|   0.92   |       0.92       |    M4    |   ModelD   |   Type3    |    130     |
+----------+------------------+----------+------------+------------+------------+
'''


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df_models, df_usage):
	# Write code here
	final_df= df_models.join(df_usage,on='Model_ID',how='inner')
    
  agg_df = df_models.groupBy("Model_Type")\
                .agg(
                    F.avg("Accuracy").alias("Average_Accuracy")
                )

  df_usage_df= df_usage.groupBy("Model_ID")\
                .agg(
                    F.sum("Uses").alias("Total_Uses")
                )


  result_df= df_models.join(agg_df,"Model_Type","inner")\
                    .join(df_usage_df,"Model_ID","inner")
     
  return result_df
