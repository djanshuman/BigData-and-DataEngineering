"""
https://datalemur.com/questions/sql-average-deal-size-2
Assuming Salesforce operates on a per user (per seat) pricing model, we have a table containing contracts data.

Write a query to calculate the average annual revenue per Salesforce customer in three market segments: SMB, Mid-Market, and Enterprise. Each customer is represented by a single contract. Format the output to match the structure shown in the Example Output section below.

Assumptions:

Yearly seat cost refers to the cost per seat.
Each customer is represented by one contract.
The market segments are categorized as:-
SMB (less than 100 employees)
Mid-Market (100 to 999 employees)
Enterprise (1000 employees or more)
The terms "average deal size" and "average revenue" refer to the same concept which is the average annual revenue generated per customer in each market segment.
contracts Table:
Column Name	Type
customer_id	integer
num_seats	integer
yearly_seat_cost	integer
contracts Example Input:
customer_id	num_seats	yearly_seat_cost
2690	50	25
4520	200	50
4520	150	50
4520	150	50
7832	878	50
customers Table:
Column Name	Type
customer_id	integer
name	varchar
employee_count	integer (0-100,000)
customers Example Input:
customer_id	name	employee_count
4520	DBT Labs	500
2690	DataLemur	99
7832	GitHub	878
Example Output:
smb_avg_revenue	mid_avg_revenue	enterprise_avg_revenue
1250	25000	43900
Explanation:
SMB Average smb_avg_revenue: DataLemur (customer ID 2690) is classified as the only SMB customer in the example data. They have a single contract with 50 seats and a yearly seat cost of $25. Therefore, the average annual revenue is: (50 * 25) / 1 = $1,250.

Mid-Market Average mid_avg_revenue: DBT Labs (customer ID 4520) is the only Mid-Market customer in the example data. They have 3 contracts with a total of 500 seats and a yearly seat cost of $50. Thus, the average annual revenue is: (500 * 50) / 1 = $25,000

Enterprise Average enterprise_avg_revenue: GitHub (customer ID 7832) is the only Enterprise customer in the example data. They have one contract with 878 seats and a yearly seat cost of $50. Therefore, the average annual revenue per Enterprise customer is: (878 * 50) / 1 = $43,900.

The dataset you are querying against may have different input & output - this is just an example!
"""
with cte as (
SELECT
  contracts.customer_id,
  name,
  employee_count,
  CASE WHEN employee_count BETWEEN 0 AND 99 THEN 'SMB'
        WHEN employee_count BETWEEN 100 AND 999 THEN 'Mid-Market'
        WHEN employee_count >= 1000 THEN 'Enterprise'
        END AS "segments",
  num_seats,
  yearly_seat_cost
  FROM customers inner join contracts
  on customers.customer_id = contracts.customer_id
  ),
  
  revenue_cte as (
  select 
      segments,
      sum(num_seats * yearly_seat_cost)/count(distinct customer_id) as "avg_revenue"
      from cte
      group by 1)
  
  -- when using AGGREGATE function result in case condition, use sum otherwise there will be null for non-matched statement
  SELECT
    sum((case when segments = 'SMB' then avg_revenue end)) as "smb_avg_revenue",
    sum((case when segments = 'Mid-Market' then avg_revenue end)) as "mid_avg_revenue",
    sum((case when segments = 'Enterprise' then avg_revenue end)) as "enterprise_avg_revenue"
    from revenue_cte;

"""
Output

smb_avg_revenue	mid_avg_revenue	enterprise_avg_revenue
2475	34450	131287

"""
