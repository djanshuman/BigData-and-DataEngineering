"""
  
  https://datalemur.com/questions/sql-top-businesses
  Assume you are given the table below containing information on user reviews. Write a query to obtain the number and percentage of businesses that are top rated. A top-rated busines is defined as one whose reviews contain only 4 or 5 stars.

Output the number of businesses and percentage of top rated businesses rounded to the nearest integer.

Assumption:

Each business has only one review (which is the business' average rating).
P.S. It's an Easy question, so keep your solution simple and short! 😉


reviews Example Input:
business_id	review_id	review_stars	review_date
532	1234	5	07/13/2022 12:00:00
824	1452	3	07/13/2022 12:00:00
819	2341	5	07/13/2022 12:00:00
716	1325	4	07/14/2022 12:00:00
423	1434	2	07/14/2022 12:00:00
  
Example Output:
business_count	top_rated_pct
3	60
Explanation: There are 3 business with the rating at least 4 - business ids 532;819;716. The total count of the businesses is 5, resulting in a percentage of 3/5 = 60%.
"""
 select 
  count(distinct business_id) as business_count,
  round(100*count(distinct business_id)/(select count(business_id) from reviews),0) 
  from  reviews where review_stars in (4,5);

"""
Output

business_count	round
9	75

"""
