"""
https://datalemur.com/questions/sql-repeat-purchases
Assume you are given the table below containing information on user purchases. Write a query to obtain the number of users who purchased the same product on two or more different days. Output the number of unique users.

PS. On 26 Oct 2022, we expanded the purchases data set, thus the official output may vary from before.

purchasesExample Input:
user_id	product_id	quantity	purchase_date
536	3223	6	01/11/2022 12:33:44
827	3585	35	02/20/2022 14:05:26
536	3223	5	03/02/2022 09:33:28
536	1435	10	03/02/2022 08:40:00
827	2452	45	04/09/2022 00:00:00
  
Example Output:
repeat_purchasers
1

"""
SELECT
  count(distinct p2.user_id) as "repeated_purchasers"
  from
  purchases p1 inner join purchases p2
  on p1.product_id = p2.product_id
  and p1.purchase_date::date != p2.purchase_date::date;

"""
Output

repeated_purchasers
2
"""
