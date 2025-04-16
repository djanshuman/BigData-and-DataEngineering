"""
https://datalemur.com/questions/sql-monthly-merchant-balance
Say you have access to all the transactions for a given merchant account. Write a query to print the cumulative balance of the merchant account at the end of each day, with the total balance reset back to zero at the end of the month. Output the transaction date and cumulative balance.

transactions Table:
Column Name	Type
transaction_id	integer
type	string ('deposit', 'withdrawal')
amount	decimal
transaction_date	timestamp
transactions Example Input:
transaction_id	type	amount	transaction_date
19153	deposit	65.90	07/10/2022 10:00:00
53151	deposit	178.55	07/08/2022 10:00:00
29776	withdrawal	25.90	07/08/2022 10:00:00
16461	withdrawal	45.99	07/08/2022 10:00:00
77134	deposit	32.60	07/10/2022 10:00:00
  
Example Output:
transaction_date	balance
07/08/2022 12:00:00	106.66
07/10/2022 12:00:00	205.16
To get cumulative balance of 106.66 on 07/08/2022 12:00:00, we take the deposit of 178.55 and minus against two withdrawals 25.90 and 45.99.

The dataset you are querying against may have different input & output - this is just an example!

"""
#Approach 1
SELECT
distinct 
date_trunc('day',transaction_date),
  sum(case when type ='deposit' then amount 
      when type ='withdrawal' then -amount
    end) over(partition by date_trunc('month',transaction_date) order by date_trunc('day',transaction_date)) as "balance"
  from transactions
  order by date_trunc('day',transaction_date);

"""
Output

date_trunc	balance
06/01/2022 00:00:00	798.69
06/05/2022 00:00:00	731.69
06/17/2022 00:00:00	896.69
06/28/2022 00:00:00	600.74
06/30/2022 00:00:00	608.24
07/02/2022 00:00:00	299.30
07/08/2022 00:00:00	405.96
07/10/2022 00:00:00	504.46
07/13/2022 00:00:00	404.46
"""

#Approach 2

with daily_balances as (
SELECT
  date_trunc('day',transaction_date) as "transaction_day",
  date_trunc('month',transaction_date) as "transaction_month",
  sum(
    (case 
    when type = 'deposit' then amount
    when type = 'withdrawal' then -amount
    end)) as "balance"
  from 
  transactions
    group by 1,2)
  SELECT
    transaction_day,
    sum(balance) over (partition by transaction_month order by transaction_day) as "balance"
    from daily_balances;
    
