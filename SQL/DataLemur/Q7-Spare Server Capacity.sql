"""
https://datalemur.com/questions/sql-spare-server-capacity
Microsoft Azure's capacity planning team wants to understand how much data its customers are using, and how much spare capacity is left in each of its data centers. You’re given three tables: customers, data centers, and forecasted_demand.

Write a query to find each data centre’s total unused server capacity. Output the data center id in ascending order and the total spare capacity.

Definitions:

monthly_capacity is the total monthly server capacity for each centers.
monthly_demand is the server demand for each customer.
P.S. If you've read the Ace the Data Science Interview and liked it, consider writing us a review?

datacenters Example Input:
datacenter_id	name	monthly_capacity
1	London	100
3	Amsterdam	250
4	Hong Kong	400

forecasted_demand Table:
customer_id	datacenter_id	monthly_demand
109	4	120
144	3	60
144	4	105
852	1	60
852	3	178
  
Example Output:
datacenter_id	spare_capacity
1	40
3	12
4	175
"""

with cte as (
SELECT
  dc.datacenter_id,
  dc.monthly_capacity,
  (dc.monthly_capacity - sum(fd.monthly_demand)) as "spare_capacity"
  from datacenters dc inner join forecasted_demand fd
  on dc.datacenter_id = fd.datacenter_id
  group by 1,2)
  SELECT
    datacenter_id,
    spare_capacity from cte 
    order by 1;

"""
datacenter_id	spare_capacity
1	22
2	55
3	12
4	150
"""
