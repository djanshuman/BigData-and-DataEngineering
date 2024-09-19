'''
PE Portfolio Values
 
You work for a PE Firm and are given two DataFrames one with a portfolio of companies that a private equity firm holds, and another that contains the daily price movements for the equities. Write a function to merge these two datasets and compute the daily portfolio value for each private equity firm.
 
Below are the schemas of the two input DataFrames and the desired output schema.
 
Input:
 
portfolio

+-------------+-----------+----------------------------------------------------+
| Column Name | Data Type |                    Description                     |
+-------------+-----------+----------------------------------------------------+
|   PE_firm   |  String   |        The name of the private equity firm         |
|   company   |  String   |              The name of the company               |
|   shares    |  Integer  | The number of shares the firm holds in the company |
+-------------+-----------+----------------------------------------------------+
 

DataFrame 2: prices

+---------------+-----------+-------------------------------------------------------+
|  Column Name  | Data Type |                      Description                      |
+---------------+-----------+-------------------------------------------------------+
|     date      |   Date    |                       The date                        |
|    company    |  String   |                The name of the company                |
| closing_price |  Double   | The closing price of the company's equity on the date |
+---------------+-----------+-------------------------------------------------------+
 
 
Output:
 
+-----------------+-----------+------------------------------------------------------+
|   Column Name   | Data Type |                     Description                      |
+-----------------+-----------+------------------------------------------------------+
|     PE_firm     |  String   |         The name of the private equity firm          |
|      date       |   Date    |                       The date                       |
| portfolio_value |  Integer  | The daily portfolio value of the private equity firm |
+-----------------+-----------+------------------------------------------------------+
 

Example

portfolio
+---------+---------+--------+
| PE_firm | company | shares |
+---------+---------+--------+
|  Alpha  |    A    |  1000  |
|  Alpha  |    B    |  2000  |
|  Beta   |    A    |  1500  |
|  Beta   |    C    |  2500  |
|  Gamma  |    B    |  1200  |
|  Gamma  |    C    |  1300  |
+---------+---------+--------+

prices
+------------+---------+---------------+
|    date    | company | closing_price |
+------------+---------+---------------+
| 2023-01-01 |    A    |     50.0      |
| 2023-01-01 |    B    |     20.0      |
| 2023-01-01 |    C    |     30.0      |
| 2023-01-02 |    A    |     52.0      |
| 2023-01-02 |    B    |     21.0      |
| 2023-01-02 |    C    |     31.0      |
+------------+---------+---------------+

Expected
+---------+------------+-----------------+
| PE_firm |    date    | portfolio_value |
+---------+------------+-----------------+
|  Alpha  | 2023-01-01 |      90000      |
|  Alpha  | 2023-01-02 |      94000      |
|  Beta   | 2023-01-01 |     150000      |
|  Beta   | 2023-01-02 |     155500      |
|  Gamma  | 2023-01-01 |      63000      |
|  Gamma  | 2023-01-02 |      65500      |
+---------+------------+-----------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(portfolio, prices):
	# Write code here
	joinedDF= portfolio.join(prices,"company","inner")

    #window_spec = W.partitionBy("PE_firm").orderBy("date")

    enrichDF= joinedDF.groupBy("PE_firm","date")\
                .agg(
                F.sum(F.col("shares").cast("double") * F.col("closing_price").cast("double")).alias("portfolio_value")
                )
    return enrichDF
