"""
https://datalemur.com/questions/sql-highest-products
Assume that you are given the table below containing information on various orders made by eBay customers. Write a query to obtain the user IDs and number of products purchased by the top 3 customers; these customers must have spent at least $1,000 in total.

Output the user id and number of products in descending order. To break ties (i.e., if 2 customers both bought 10 products), the user who spent more should take precedence.

user_transactions Example Input:
transaction_id	product_id	user_id	spend
131432	1324	128	699.78
131433	1313	128	501.00
153853	2134	102	1001.20
247826	8476	133	1051.00
247265	3255	133	1474.00
136495	3677	133	247.56
  
Example Output:
user_id	product_num
133	3
128	2
102	1
"""

--solution 1--
SELECT
  user_id,
  count(product_id) as "product_num"
  from user_transactions
  group by 1
  having sum(spend) >= 1000
  order by count(product_id) desc , sum(spend) desc
  limit 3;

--solution 2 --
with cte as (
SELECT
  user_id,
  count(product_id) as "product_num",
  sum(spend),
  rank() over (order by count(product_id) desc, sum(spend) desc ) as "rnk"
  from user_transactions
  group by 1
  having sum(spend) >= 1000)
  select 
    user_id,
    product_num
    from cte where rnk <=3;

"""
Output

user_id	product_num
133	4
128	2
173	1
"""
