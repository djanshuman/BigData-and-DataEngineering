'''
GDP Growth Rate
 

As an Economist and need to compute the annual growth rate of GDP from multiple economic DataFrames. The GDP growth rate is the percentage increase in a country's GDP from one year to the next. It is calculated by using the formula:
 
GDP growth rate = [(GDP this year - GDP last year) / GDP last year] * 100
 
You have been provided with two DataFrames.
 
The first, df1, has the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|   Country   |  String   |
|    Year     |  Integer  |
|     GDP     |  Double   |
+-------------+-----------+
 

The second, df2, also contains economic data with the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
|   Country   |  String   |
|    Year     |  Integer  |
|     GDP     |  Double   |
+-------------+-----------+
 

These DataFrames can contain data for different countries and different years.
 
Write a function that combines these DataFrames and returns the annual GDP growth rate for each country and each year.
 
The output should have the following schema:

+-----------------+-----------+
|   Column Name   | Data Type |
+-----------------+-----------+
|     Country     |  String   |
|      Year       |  Integer  |
| GDP_growth_rate |  Double   |
+-----------------+-----------+
 

Constraints:
 
The output should be sorted in ascending order first by country name and then by year.
 
The GDP growth rate should be rounded off to two decimal places.
 
If the GDP data for the previous year is not available, the GDP growth rate for the current year should be null.
 
You can assume that the data in both the input DataFrames is clean, i.e., there are no missing values and the GDP is always greater than or equal to zero.
 
 

Example

df1
+---------+------+----------+
| Country | Year |   GDP    |
+---------+------+----------+
|   USA   | 2018 | 20544.34 |
|   USA   | 2019 | 21427.7  |
|  China  | 2018 | 13894.04 |
+---------+------+----------+

df2
+---------+------+----------+
| Country | Year |   GDP    |
+---------+------+----------+
|  China  | 2019 | 14402.72 |
|  India  | 2018 | 2713.61  |
|  India  | 2019 | 2868.93  |
+---------+------+----------+

Expected
+---------+-----------------+------+
| Country | GDP_growth_rate | Year |
+---------+-----------------+------+
|  China  |       nan       | 2018 |
|   USA   |       0.0       | 2018 |
|   USA   |       0.0       | 2019 |
|   USA   |      2.61       | 2016 |
|   USA   |      3.63       | 2013 |
|   USA   |      3.67       | 2011 |
|   USA   |      4.06       | 2015 |
|   USA   |      4.21       | 2012 |
|   USA   |       4.3       | 2019 |
|   USA   |      4.42       | 2014 |
|   USA   |      4.42       | 2017 |
|   USA   |      5.13       | 2018 |
|   USA   |       nan       | 2010 |
+---------+-----------------+------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(df1, df2):
	# Write code here
	joinedDF = df1.union(df2)

    window_spec = W.partitionBy("Country").orderBy("Year")

    enrichDF = joinedDF.withColumn("prev_year_GDP",
                        F.lag(F.col("GDP")).over(window_spec)
                                  )
  
    enrichDF = enrichDF.withColumn("GDP_growth_rate",
                    F.when(
                            F.isnull("prev_year_GDP"),None)\
                            .otherwise(
                                (F.col("GDP")-F.col("prev_year_gdp")) 
                                 / F.col("prev_year_gdp")
                                *100
                            ))
    final_df= enrichDF.select("Country",
                            F.round("GDP_growth_rate",2).alias("GDP_growth_rate"),
                             "Year")
    return final_df
