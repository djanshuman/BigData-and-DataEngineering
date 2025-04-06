"""
https://datalemur.com/questions/final-account-balance
Given a table containing information about bank deposits and withdrawals made using Paypal, write a query to retrieve the final account balance for each account, taking into account all the transactions recorded in the table with the assumption that there are no missing transactions.

transactions Table:
Column Name	Type
transaction_id	integer
account_id	integer
amount	decimal
transaction_type	varchar
  
transactions Example Input:
  
transaction_id	account_id	amount	transaction_type
123	101	10.00	Deposit
124	101	20.00	Deposit
125	101	5.00	Withdrawal
126	201	20.00	Deposit
128	201	10.00	Withdrawal
  
Example Output:
account_id	final_balance
101	25.00
201	10.00


"""

--solution 1 --

with cte as(
select 
  account_id,
  transaction_id,
  amount,
  (case 
      when transaction_type = 'Deposit' then 'D'
      when transaction_type = 'Withdrawal' then 'W'
      end) as "transaction_category"
  from transactions),
  transactions as (SELECT
    account_id,
    transaction_category,
    sum(case when transaction_category = 'D' then amount end ) as "deposit_final_sum",
    sum(case when transaction_category = 'W' then amount end ) as "withdraw_final_sum"
    from cte
    group by 1,2)
    SELECT
      account_id,
      sum(deposit_final_sum) - sum(withdraw_final_sum) as "final_balance"
      from transactions
      group by 1;



--solution 2 --

SELECT
  account_id,
  sum(
      case when transaction_type ='Deposit' then amount
      else -amount end) as "final_balance"
  from transactions
  group by account_id;
  

