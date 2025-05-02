"""
https://datalemur.com/questions/photoshop-revenue-analysis
For every customer that bought Photoshop, return a list of the customers, and the total spent on all the products except for Photoshop products.

Sort your answer by customer ids in ascending order.

adobe_transactions Table:
Column Name	Type
customer_id	integer
product	string
revenue	integer
adobe_transactions Example Input:
customer_id	product	revenue
123	Photoshop	50
123	Premier Pro	100
123	After Effects	50
234	Illustrator	200
234	Premier Pro	100
Example Output:
customer_id	revenue
123	150
Explanation: User 123 bought Photoshop, Premier Pro + After Effects, spending $150 for those products. We don't output user 234 because they didn't buy Photoshop.

The dataset you are querying against may have different input & output - this is just an example!
"""
--solution 1--
select 
  customer_id,
  sum(revenue) as "revenue"
  from adobe_transactions 
  where exists(
            select customer_id
            from adobe_transactions adobe
            where adobe_transactions.customer_id = adobe.customer_id
            and adobe.product = 'Photoshop'
            )
  and product != 'Photoshop'
  group by 1;

--solution 2 --
with cte as (
SELECT
  customer_id
  from 
  adobe_transactions where product = 'Photoshop')
select 
  cte.customer_id,
  sum(adobe.revenue) as "revenue"
  from adobe_transactions adobe inner join cte
  on adobe.customer_id= cte.customer_id
  where adobe.product != 'Photoshop'
  group by 1;

"""
output:
customer_id	revenue
123	150
913	300
"""
