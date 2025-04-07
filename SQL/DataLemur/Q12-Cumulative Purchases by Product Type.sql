"""
https://datalemur.com/questions/sql-purchasing-activity

Assume you're given a table containing Amazon purchasing activity. Write a query to calculate the cumulative purchases for each product type, ordered chronologically.

The output should consist of the order date, product, and the cumulative sum of quantities purchased.


total_trans Example Input:
order_id	product_type	quantity	order_date
213824	printer	20	06/27/2022 12:00:00
132842	printer	18	06/28/2022 12:00:00
  
Example Output:
order_date	product_type	cum_purchased
06/27/2022 12:00:00	printer	20
06/28/2022 12:00:00	printer	38
Explanation:
On June 27, 2022, a total of 20 printers were purchased. Following that, on June 28, 2022, an additional 18 printers were purchased, resulting in a cumulative total of 38 printers (20 + 18).
"""
SELECT
  order_date,
  product_type,
  sum(quantity) over(partition by product_type order by order_date
        rows between unbounded preceding and current row ) as "cumulative_purchases"
  from total_trans
  order by 1;
"""
Output

order_date	product_type	cumulative_purchases
06/27/2022 12:00:00	printer	20
06/28/2022 12:00:00	hair dryer	5
06/28/2022 12:00:00	printer	38
07/05/2022 12:00:00	standing lamp	8
07/05/2022 12:00:00	printer	63
07/06/2022 12:00:00	standing lamp	44
09/15/2022 12:00:00	standing lamp	54
09/16/2022 12:00:00	printer	78
09/20/2022 12:00:00	hair dryer	30
09/26/2022 12:00:00	printer	90

"""
