"""

https://datalemur.com/questions/same-week-purchases
You are given the two tables containing information on Etsy’s user signups and purchases. Write a query to obtain the percentage of users who signed up and made a purchase within 7 days of signing up. The result should be rounded to the nearest 2 decimal places.

Assumptions:

Signups who didn't buy any products yet should also count as part of the percentage of users who signed up and made a purchase within 7 days of signing up
If the signup date is on 06/21/2022 and the purchase date on 06/26/2022, then the user makes up part of the percentage of users who signed up and purchased within the 7 days of signing up.
signups Table:
Column Name	Type
user_id	integer
signup_date	datetime

signups Example Input:
user_id	signup_date
445	06/21/2022 12:00:00
742	06/19/2022 12:00:00
648	06/24/2022 12:00:00
789	06/27/2022 12:00:00
123	06/27/2022 12:00:00
  
user_purchases Table:
user_purchases Example Input:
user_id	product_id	purchase_amount	purchase_date
244	7575	45.00	06/22/2022 12:00:00
742	1241	50.00	06/28/2022 12:00:00
648	3632	55.50	06/25/2022 12:00:00
123	8475	67.30	06/29/2022 12:00:00
244	2341	74.10	06/30/2022 12:00:00

  
Example Output:
single_purchase_pct
40.00
Explanation: The only users who purchased within 7 days of signing up are users 648 and 123. The total count of given signups is 5, resulting in a percentage of 2/5 = 40%.

"""


SELECT
  round
    (100*count(distinct  user_purchases.user_id)
    /count(distinct signups.user_id),2) as single_purchase_pct
  from
  signups left outer join user_purchases
  on signups.user_id = user_purchases.user_id
  where user_purchases.purchase_date is NULL OR
  user_purchases.purchase_date BETWEEN
  signups.signup_date and (signups.signup_date + '7 days':: interval);


"""

Output

single_purchase_pct
70.00
"""
