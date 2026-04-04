'''
Amazon is trying to identify their high-end customers. To do so, they first need your help to write a query that obtains the most expensive purchase made by each customer. Order the results by the most expensive purchase first.

transactions Table:

Column Name	Type
transaction_id	integer
customer_id	integer
purchase_amount	integer
transactions Example Input:

transaction_id	customer_id	purchase_amount
1	1	150
2	1	35.90
3	2	349.99
4	2	199.95
5	2	551.20
6	3	13.30
Example Output:

customer_id	purchase_amount
2	551.20
1	150
3	13.30
Explanation:

User 1 have 2 purchases (150 and 35.90) with 150 being the most expensive purchase whereas user 2 have 3 purchases (349.99, 199.95, and 551.20) and 551.20 is their most expensive purchase.

User 3 only have 1 purchase at 13.30 and hence its their most expensive purchase. The output is then sorted by the most expensive purchase to the least expensive.

The dataset you are querying against may have different input & output - this is just an example!

'''

with cte as (
SELECT 
  customer_id,
  purchase_amount,
  rank() over(partition by customer_id order by purchase_amount desc) as rank
  FROM transactions)
    select  
      customer_id,
      purchase_amount
      from cte where rank = 1 
      order by purchase_amount desc;
