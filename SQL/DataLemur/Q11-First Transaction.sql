"""
https://datalemur.com/questions/sql-first-transaction
Assume you're given a table containing Etsy user transactions. Write a query that retrieves the customers whose first transaction was valued at $50 or more. Output the total number of users who meet this criteria.

Instructions:

To determine the first transaction for each user, use the transaction_date field.
Take into account scenarios where a user had multiple transactions on the same day. Use a specific function (we can't give too much away 😉) to handle these cases and correctly identify the first transaction.
Effective June 14th, 2023, the solution and hints have been revised.

user_transactions Example Input:
transaction_id	user_id	spend	transaction_date
759274	111	49.50	02/03/2022 00:00:00
850371	111	51.00	03/15/2022 00:00:00
615348	145	36.30	03/22/2022 00:00:00
137424	156	151.00	04/04/2022 00:00:00
248475	156	87.00	04/16/2022 00:00:00
  
Example Output:
users
1
"""
with cte as (
select 
  user_id,
  spend,
  transaction_date,
  row_number() over (partition by user_id order by transaction_date) as "rownum"
  FROM
  user_transactions)
  SELECT
    count(distinct user_id) as users
    from cte WHERE rownum =1 
    and spend >= 50;

"""
Output
users
3
"""
