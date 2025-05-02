"""
https://datalemur.com/questions/alibaba-compressed-median

You are trying to find the median number of items bought per order on Alibaba, rounded to 1 decimal point.

However, instead of doing analytics on all Alibaba orders, you have access to a summary table, which describes how many items were in an order, and the number of orders that had that many items.

items_per_order Table:
Column Name	Type
item_count	integer
order_occurrences	integer
items_per_order Example Input:
item_count	order_occurrences
1	500
2	1000
3	800
4	1000
  
Example Output:
median
3.0
  
Explanation
The total orders in the order_occurrences field in this dataset is 3300, meaning that the median item count would be for the 1650th order (3300 / 2 = 1650).

If we compare this to the running sum of order_occurrences, we can see that the median item count is 3.

The dataset you are querying against may have different input & output - this is just an example!

"""
with cte as(
SELECT
 *,
  sum(order_occurrences) over(order by item_count) as "running_sum",
  sum(order_occurrences) over()  as "total_sum"
  from items_per_order)
    select round(avg(item_count)::Decimal,1) as "median"
      from cte
    WHERE total_sum <= 2 * running_sum
    AND total_sum >= 2 * (running_sum - order_occurrences);

"""
Output

median
4.0
"""
