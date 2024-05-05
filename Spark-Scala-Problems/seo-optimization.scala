/*  
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
*/

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark
import java.time._

val spark = SparkSession.builder().appName("run-spark-code").getOrCreate()

import spark.implicits._

def etl(pages: DataFrame): DataFrame = {
	// generate DF with highest seo score per domain
    val window_spec= Window.partitionBy("domain").orderBy(desc("seo_score"))
    val highest_seo_score_rows=pages.withColumn("rank",row_number() over(window_spec))
    val highest_by_domain_df=highest_seo_score_rows.filter("rank == 1").drop("rank")
    
    //calculate overall highest seo score
    val highest_seo_score=pages
        .agg(max("seo_score").as("o_high_seo_score"))

    //pull the pages havng overall highest seo score
    val highest_score_pages=pages
        .join(highest_seo_score,highest_seo_score("o_high_seo_score") === pages("seo_score"),"inner")
    highest_score_pages

    val highest_score_pages_df= highest_score_pages.selectExpr(
        "domain as o_domain",
        "url as overall_highest_page",
        "seo_score as overall_highest_score"
    )
    val joinedDF= highest_by_domain_df
    .join(highest_score_pages_df,
          highest_by_domain_df("domain") === highest_score_pages_df("o_domain"),"left")

    //joinedDF
    val resultDF = joinedDF.selectExpr(
        "domain",
        "url as highest_seo_page",
        "seo_score as highest_seo_score",
        "overall_highest_page",
        "overall_highest_score"    
    )
    //joinedDF
    resultDF
    
    
}
