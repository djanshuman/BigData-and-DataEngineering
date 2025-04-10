"""
  https://datalemur.com/questions/sql-bad-experience

  
The Growth Team at DoorDash wants to ensure that new users, who make orders within their first 14 days on the platform, have a positive experience. However, they have noticed several issues with deliveries that result in a bad experience.

These issues include:

Orders being completed incorrectly, with missing items or wrong orders.
Orders not being received due to incorrect addresses or drop-off spots.
Orders being delivered late, with the actual delivery time being 30 minutes later than the order placement time. Note that the estimated_delivery_timestamp is automatically set to 30 minutes after the order_timestamp.
Write a query that calculates the bad experience rate for new users who signed up in June 2022 during their first 14 days on the platform. The output should include the percentage of bad experiences, rounded to 2 decimal places.


orders Table:
Column Name	Type
order_id	integer
customer_id	integer
trip_id	integer
status	string ('completed successfully', 'completed incorrectly', 'never received')
order_timestamp	timestamp
  
orders Example Input:
order_id	customer_id	trip_id	status	order_timestamp
727424	8472	100463	completed successfully	06/05/2022 09:12:00
242513	2341	100482	completed incorrectly	06/05/2022 14:40:00
141367	1314	100362	completed incorrectly	06/07/2022 15:03:00
582193	5421	100657	never_received	07/07/2022 15:22:00
253613	1314	100213	completed successfully	06/12/2022 13:43:00
  
trips Table:
Column Name	Type
dasher_id	integer
trip_id	integer
estimated_delivery_timestamp	timestamp
actual_delivery_timestamp	timestamp
  
trips Example Input:
dasher_id	trip_id	estimated_delivery_timestamp	actual_delivery_timestamp
101	100463	06/05/2022 09:42:00	06/05/2022 09:38:00
102	100482	06/05/2022 15:10:00	06/05/2022 15:46:00
101	100362	06/07/2022 15:33:00	06/07/2022 16:45:00
102	100657	07/07/2022 15:52:00	-
103	100213	06/12/2022 14:13:00	06/12/2022 14:10:00
  
customers Table:
Column Name	Type
customer_id	integer
signup_timestamp	timestamp
  
customers Example Input:
customer_id	signup_timestamp
8472	05/30/2022 00:00:00
2341	06/01/2022 00:00:00
1314	06/03/2022 00:00:00
1435	06/05/2022 00:00:00
5421	06/07/2022 00:00:00
  
Example Output:
bad_experience_pct
75.00
  
Explanation:
Order 727424 is excluded from the analysis as it was placed after the first 14 days upon signing up.

Out of the remaining orders, there are a total of 4 orders. However, only 3 of these orders resulted in a bad experience, as one order with ID 253613 was completed successfully. Therefore, the bad experience rate is 75% (3 out of 4 orders).

The dataset you are querying against may have different input & output - this is just an example!
"""


with base as (
select 
  c.customer_id,
  o.order_id,
  o.trip_id,
  o.status,
  c.signup_timestamp,
  o.order_timestamp
  from customers c inner join orders o on o.customer_id = c.customer_id
  inner join trips t on t.trip_id= o.trip_id
  where c.signup_timestamp between '2022-06-01' and '2022-06-30'
  and o.order_timestamp between c.signup_timestamp and  c.signup_timestamp + INTERVAL '14 DAYS'
  order by c.customer_id, o.order_id),
  agg as (
    SELECT
      round(100.0*sum(case when status in ('completed incorrectly','never received') then 1 else 0 end)/
      count(order_id),2) as "bad_experience_pct"
      from base          
    )select * from agg;
"""
Output

bad_experience_pct
57.14
"""
