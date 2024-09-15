'''
AI Research


You are given two DataFrames containing information about Artificial Intelligence (AI) research papers and their respective authors. Write a function that combines the DataFames and assigns a unique row number to each author and is partitioned by their research paper ID. 


The input DataFrames have the following schemas:

research_papers

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| paper_id    | string    |
| title       | string    |
| year        | integer   |
+-------------+-----------+

authors

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| paper_id    | string    |
| author_id   | string    |
| name        | string    |
+-------------+-----------+

The output DataFrame should have the following schema:

+-------------+-----------+
| Column Name | Data Type |
+-------------+-----------+
| paper_id    | string    |
| author_id   | string    |
| name        | string    |
| row_number  | integer   |
+-------------+-----------+

 

 

Example


research_papers
+----------+--------------------------------------+------+
| paper_id | title                                | year |
+----------+--------------------------------------+------+
| P1       | Deep Learning Techniques in AI       | 2019 |
| P2       | Reinforcement Learning for Robotics  | 2020 |
| P3       | Natural Language Processing Advances | 2021 |
+----------+--------------------------------------+------+

authors
+----------+-----------+----------------+
| paper_id | author_id | name           |
+----------+-----------+----------------+
| P1       | A1        | Alice Smith    |
| P1       | A2        | Bob Johnson    |
| P2       | A3        | Carol Williams |
| P2       | A4        | David Brown    |
| P2       | A5        | Eva Davis      |
| P3       | A6        | Frank Wilson   |
| P3       | A7        | Grace Lee      |
+----------+-----------+----------------+

Output
+-----------+----------------+----------+------------+
| author_id | name           | paper_id | row_number |
+-----------+----------------+----------+------------+
| A1        | Alice Smith    | P1       | 1          |
| A2        | Bob Johnson    | P1       | 2          |
| A3        | Carol Williams | P2       | 1          |
| A4        | David Brown    | P2       | 2          |
| A5        | Eva Davis      | P2       | 3          |
| A6        | Frank Wilson   | P3       | 1          |
| A7        | Grace Lee      | P3       | 2          |
+-----------+----------------+----------+------------+
'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(research_papers, authors):
	# Write code here
	combinedDF= research_papers.join(authors,"paper_id")
  window_spec= W.partitionBy("paper_id").orderBy("author_id")

  resultDF= combinedDF.select(
                "author_id",
                "name",
                "paper_id",
                F.row_number().over(window_spec).alias("row_number")
  )
  return resultDF
