'''
SEO Optimization



You are given a DataFrame containing information about webpages and their SEO (Search Engine Optimization) scores. Write a function that returns the pages with the highest SEO score for each domain, and also the pages with the highest SEO score among all domains.



The input DataFrame pages has the following schema (Drag panel to right to view tables in full):

+-------------+-----------+--------------------------------+
| Column Name | Data Type | Description                    |
+-------------+-----------+--------------------------------+
| domain      | string    | The domain name of the webpage |
| url         | string    | The URL of the webpage         |
| seo_score   | integer   | The SEO score of the webpage   |
+-------------+-----------+--------------------------------+


Output Schema:

+-----------------------+-----------+---------------------------------------------------------------------------+
| Column Name           | Data Type | Description                                                               |
+-----------------------+-----------+---------------------------------------------------------------------------+
| domain                | string    | The domain name of the webpage                                            |
| highest_seo_page      | string    | The URL of the webpage with the highest SEO score within the domain       |
| highest_seo_score     | integer   | The SEO score of the webpage with the highest SEO score within the domain |
| overall_highest_page  | string    | The URL of the webpage with the highest SEO score among all domains       |
| overall_highest_score | integer   | The SEO score of the webpage with the highest SEO score among all domains |
+-----------------------+-----------+---------------------------------------------------------------------------+


Constraints:

The input DataFrame will have at least 1 row and at most 10^6 rows.
The seo_score column will have values in the range of 0 to 100 (inclusive).
The domain and url columns will have at most length 255.


Example



pages
+-------------+-------------------------------+-----------+
| domain      | url                           | seo_score |
+-------------+-------------------------------+-----------+
| example.com | https://www.example.com/page1 | 88        |
| example.com | https://www.example.com/page2 | 92        |
| example.com | https://www.example.com/page3 | 80        |
| example.net | https://www.example.net/page1 | 75        |
| example.net | https://www.example.net/page2 | 90        |
| example.org | https://www.example.org/page1 | 82        |
| example.org | https://www.example.org/page2 | 85        |
+-------------+-------------------------------+-----------+

Output
+-------------+-------------------------------+-------------------+-------------------------------+-----------------------+
| domain      | highest_seo_page              | highest_seo_score | overall_highest_page          | overall_highest_score |
+-------------+-------------------------------+-------------------+-------------------------------+-----------------------+
| example.com | https://www.example.com/page2 | 92                | https://www.example.com/page2 | 92.0                  |
| example.net | https://www.example.net/page2 | 90                | None                          | NaN                   |
| example.org | https://www.example.org/page2 | 85                | None                          | NaN                   |
+-------------+-------------------------------+-------------------+-------------------------------+-----------------------+
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(pages):
	# Write code here
    # highest seo page
	domain_window= W.partitionBy("domain").orderBy(F.desc("seo_score"))
  pages_with_rank = pages\
                    .withColumn("rank",F.row_number().over(domain_window))
  highest_seo_page = pages_with_rank.filter(F.col("rank") == 1).drop("rank")
    
  # highest seo score
  pages_with_high_seo_score = pages.agg(
                            F.max("seo_score").alias("overall_highest_score")
                            )
  highest_page= pages.join(pages_with_high_seo_score,
                    pages["seo_score"] == pages_with_high_seo_score["overall_highest_score"]
                                                            ,"inner")
  # return highest_page
  highest_overall_seo_page= highest_page.selectExpr(
                    "domain as o_domain",
                    "seo_score as overall_highest_score",
                    "url as overall_highest_page")
    
  finalDF= highest_seo_page.join(highest_overall_seo_page,
                                  highest_seo_page["domain"] == highest_overall_seo_page["o_domain"],
                                  "left").selectExpr(
                                    "domain",
                                    "url as highest_seo_page",
                                    "seo_score as highest_seo_score",
                                    "overall_highest_page",
                                    "overall_highest_score"
                        )
  return finalDF

