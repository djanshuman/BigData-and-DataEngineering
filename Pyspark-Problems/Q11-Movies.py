'''
Movies

 

You have been given a DataFrame movies_df containing information about the movies released in the year 2022. The schema of movies_df is as follows:

 

movies_df

 

+-----------------------+-----------+------------------------------------+
| Column Name           | Data Type | Description                        |
+-----------------------+-----------+------------------------------------+
| movie_id              | integer   | unique id of the movie             |
| movie_title           | string    | title of the movie                 |
| director_name         | string    | name of the movie director         |
| release_date          | date      | date when the movie was released   |
| box_office_collection | float     | box office collection of the movie |
| genre                 | string    | genre of the movie                 |
+-----------------------+-----------+------------------------------------+
 

Filter movies_df to retain only the rows where the box_office_collection column is null.

 

Result Schema

 

+-----------------------+-----------+
| Column Name           | Data Type |
+-----------------------+-----------+
| movie_id              | integer   |
| movie_title           | string    |
| director_name         | string    |
| release_date          | date      |
| box_office_collection | float     |
| genre                 | string    |
+-----------------------+-----------+



Example (Drag Panel to right to View full)

 

movies_df
+----------+---------------------------------------------+-----------------+--------------+-----------------------+------------------------------------+
| movie_id | movie_title                                 | director_name   | release_date | box_office_collection | genre                              |
+----------+---------------------------------------------+-----------------+--------------+-----------------------+------------------------------------+
| 1        | The Avengers                                | Joss Whedon     | 2022-05-06   | 1856.45               | Action, Adventure, Sci-Fi          |
| 2        | Black Panther: Wakanda Forever              | Ryan Coogler    | 2022-11-10   | NULL                  | Action, Adventure, Drama           |
| 3        | Jurassic World: Dominion                    | Colin Trevorrow | 2022-06-10   | 1234.56               | Action, Adventure, Sci-Fi          |
| 4        | Fantastic Beasts and Where to Find Them 3   | David Yates     | 2022-11-04   | NULL                  | Adventure, Family, Fantasy         |
| 5        | Sonic the Hedgehog 2                        | Jeff Fowler     | 2022-04-08   | 789.12                | Action, Adventure, Comedy          |
| 6        | The Batman                                  | Matt Reeves     | 2022-03-04   | 2345.67               | Action, Adventure, Crime           |
| 7        | Avatar 2                                    | James Cameron   | 2022-12-16   | 5678.90               | Action, Adventure, Fantasy, Sci-Fi |
| 8        | Doctor Strange in the Multiverse of Madness | Sam Raimi       | 2022-03-25   | 4567.89               | Action, Adventure, Fantasy         |
+----------+---------------------------------------------+-----------------+--------------+-----------------------+------------------------------------+


Output
+-----------------------+---------------+----------------------------+----------+-------------------------------------------+--------------+
| box_office_collection | director_name | genre                      | movie_id | movie_title                               | release_date |
+-----------------------+---------------+----------------------------+----------+-------------------------------------------+--------------+
| NULL                  | Ryan Coogler  | Action, Adventure, Drama   | 2        | Black Panther: Wakanda Forever            | 2022-11-10   |
| NULL                  | David Yates   | Adventure, Family, Fantasy | 4        | Fantastic Beasts and Where to Find Them 3 | 2022-11-04   |
+-----------------------+---------------+----------------------------+----------+-------------------------------------------+--------------+

'''

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
import pyspark
import datetime
import json

spark = SparkSession.builder.appName('run-pyspark-code').getOrCreate()

def etl(movies_df):
	# Write code here
	resultDF= movies_df.filter(F.isnull("box_office_collection"))
  return resultDF
