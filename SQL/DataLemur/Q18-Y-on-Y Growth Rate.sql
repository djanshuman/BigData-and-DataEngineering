"""
Assume you're given a table containing information about Wayfair user transactions for different products. Write a query to calculate the year-on-year growth rate for the total spend of each product, grouping the results by product ID.

The output should include the year in ascending order, product ID, current year's spend, previous year's spend and year-on-year growth percentage, rounded to 2 decimal places.

user_transactions Table:
Column Name	Type
transaction_id	integer
product_id	integer
spend	decimal
transaction_date	datetime
  
user_transactions Example Input:
transaction_id	product_id	spend	transaction_date
1341	123424	1500.60	12/31/2019 12:00:00
1423	123424	1000.20	12/31/2020 12:00:00
1623	123424	1246.44	12/31/2021 12:00:00
1322	123424	2145.32	12/31/2022 12:00:00
  
Example Output:
year	product_id	curr_year_spend	prev_year_spend	yoy_rate
2019	123424	1500.60	NULL	NULL
2020	123424	1000.20	1500.60	-33.35
2021	123424	1246.44	1000.20	24.62
2022	123424	2145.32	1246.44	72.12

"""
with cte as (
SELECT 
  extract(year from transaction_date) as "year",
  product_id,
  spend as "curr_year_spend",
  lag(spend) over (partition by product_id order by extract(year from transaction_date)
    rows between unbounded preceding and current row) as "prev_year_spend"
  FROM user_transactions)
  SELECT
    year,
    product_id,
    curr_year_spend,
    prev_year_spend,
    round(100*(curr_year_spend - prev_year_spend)/prev_year_spend,2) as "yoy_rate"
    from cte;

"""
  Output

year	product_id	curr_year_spend	prev_year_spend	yoy_rate
2019	123424	1500.60	NULL	NULL
2020	123424	1000.20	1500.60	-33.35
2021	123424	1246.44	1000.20	24.62
2022	123424	2145.32	1246.44	72.12
2019	234412	1800.00	NULL	NULL
2020	234412	1234.00	1800.00	-31.44
2021	234412	889.50	1234.00	-27.92
2022	234412	2900.00	889.50	226.03
2019	543623	6450.00	NULL	NULL
2020	543623	5348.12	6450.00	-17.08
2021	543623	2345.00	5348.12	-56.15
2022	543623	5680.00	2345.00	142.22
"""
