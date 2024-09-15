'''
Private Equity Firms

 

You have been given three DataFrames representing information about Private Equity (PE) firms, their funds, and their investments. Write a function that combines all three DataFrames. Please note that some of the values may be null, as not all investments are associated with a fund, and not all funds are associated with a firm. Additionally, any rows where all columns are null should be filtered out.


pe_firms, has the following schema:

+--------------+-----------+--------------------------------------+
| Column Name  | Data Type | Description                          |
+--------------+-----------+--------------------------------------+
| firm_id      | integer   | the unique identifier of the PE firm |
| firm_name    | string    | the name of the PE firm              |
| founded_year | integer   | the year the PE firm was founded     |
| location     | string    | the location of the PE firm          |
+--------------+-----------+--------------------------------------+

pe_funds, has the following schema:

+-----------------+-----------+--------------------------------------------------+
| Column Name     | Data Type | Description                                      |
+-----------------+-----------+--------------------------------------------------+
| fund_id         | integer   | the unique identifier of the PE fund             |
| firm_id         | integer   | the unique identifier of the PE firm             |
| fund_name       | string    | the name of the PE fund                          |
| fund_size       | integer   | the size of the PE fund in millions of dollars   |
| fund_start_year | integer   | the year the PE fund was started                 |
| fund_end_year   | integer   | the year the PE fund ended or is expected to end |
+-----------------+-----------+--------------------------------------------------+

pe_investments, has the following schema:

+-------------------+-----------+-----------------------------------------------------+
| Column Name       | Data Type | Description                                         |
+-------------------+-----------+-----------------------------------------------------+
| investment_id     | integer   | the unique identifier of the PE investment          |
| fund_id           | integer   | the unique identifier of the PE fund                |
| company_name      | string    | the name of the company receiving the investment    |
| investment_amount | integer   | the amount of the investment in millions of dollars |
| investment_date   | string    | the date of the investment                          |
+-------------------+-----------+-----------------------------------------------------+

The resulting DataFrame should have the following schema:

+-------------------+-----------+-----------------------------------------------------+
| Column Name       | Data Type | Description                                         |
+-------------------+-----------+-----------------------------------------------------+
| investment_id     | integer   | the unique identifier of the PE investment          |
| fund_id           | integer   | the unique identifier of the PE fund                |
| firm_id           | integer   | the unique identifier of the PE firm                |
| firm_name         | string    | the name of the PE firm                             |
| founded_year      | integer   | the year the PE firm was founded                    |
| location          | string    | the location of the PE firm                         |
| fund_name         | string    | the name of the PE fund                             |
| fund_size         | integer   | the size of the PE fund in millions of dollars      |
| fund_start_year   | integer   | the year the PE fund was started                    |
| fund_end_year     | integer   | the year the PE fund ended or is expected to end    |
| company_name      | string    | the name of the company receiving the investment    |
| investment_amount | integer   | the amount of the investment in millions of dollars |
| investment_date   | string    | the date of the investment                          |
+-------------------+-----------+-----------------------------------------------------+
 

 

 

Example

 

pe_firms
+---------+-----------+--------------+----------+
| firm_id | firm_name | founded_year | location |
+---------+-----------+--------------+----------+
| 1       | ABC Fund  | 2010         | New York |
| 2       | XYZ Fund  | 2005         | London   |
| 3       | DEF Fund  | 2015         | Paris    |
+---------+-----------+--------------+----------+

pe_funds
+---------+---------+-----------+-----------+-----------------+---------------+
| fund_id | firm_id | fund_name | fund_size | fund_start_year | fund_end_year |
+---------+---------+-----------+-----------+-----------------+---------------+
| 101     | 1       | ABC I     | 100       | 2010            | 2015          |
| 102     | 1       | ABC II    | 150       | 2015            | 2020          |
| 103     | 2       | XYZ I     | 200       | 2010            | 2018          |
+---------+---------+-----------+-----------+-----------------+---------------+

pe_investments
+---------------+---------+--------------+-------------------+-----------------+
| investment_id | fund_id | company_name | investment_amount | investment_date |
+---------------+---------+--------------+-------------------+-----------------+
| 1001          | 101     | Company A    | 10                | 2012-05-15      |
| 1002          | 101     | Company B    | 20                | 2013-06-20      |
| 1003          | 102     | Company C    | 30                | 2016-07-25      |
+---------------+---------+--------------+-------------------+-----------------+

Output (Slide panel to right to view full)
+--------------+---------+-----------+--------------+---------------+---------+-----------+-----------+-----------------+-------------------+-----------------+---------------+----------+
| company_name | firm_id | firm_name | founded_year | fund_end_year | fund_id | fund_name | fund_size | fund_start_year | investment_amount | investment_date | investment_id | location |
+--------------+---------+-----------+--------------+---------------+---------+-----------+-----------+-----------------+-------------------+-----------------+---------------+----------+
| Company A    | 1       | ABC Fund  | 2010         | 2015.0        | 101.0   | ABC I     | 100.0     | 2010.0          | 10.0              | 2012-05-15      | 1001.0        | New York |
| Company B    | 1       | ABC Fund  | 2010         | 2015.0        | 101.0   | ABC I     | 100.0     | 2010.0          | 20.0              | 2013-06-20      | 1002.0        | New York |
| Company C    | 1       | ABC Fund  | 2010         | 2020.0        | 102.0   | ABC II    | 150.0     | 2015.0          | 30.0              | 2016-07-25      | 1003.0        | New York |
| None         | 2       | XYZ Fund  | 2005         | 2018.0        | 103.0   | XYZ I     | 200.0     | 2010.0          | NaN               | None            | NaN           | London   |
| None         | 3       | DEF Fund  | 2015         | NaN           | NaN     | None      | NaN       | NaN             | NaN               | None            | NaN           | Paris    |
+--------------+---------+-----------+--------------+---------------+---------+-----------+-----------+-----------------+-------------------+-----------------+---------------+----------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pe_firms, pe_funds, pe_investments):
	# Write code here
	resultDF= pe_firms.join(pe_funds,"firm_id","outer").\
                    join(pe_investments,"fund_id","outer")
  finalDF = resultDF.dropna(how="all")
  return finalDF
