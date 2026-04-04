'''
  Who Made Quota?
Oracle SQL Interview Question
Question
Solution
Discussion
Submissions
As a data analyst on the Oracle Sales Operations team, you are given a list of salespeople’s deals, and the annual quota they need to hit.

Write a query that outputs each employee id and whether they hit the quota or not ('yes' or 'no'). Order the results by employee id in ascending order.

Definitions:

deal_size: Deals acquired by a salesperson in the year. Each salesperson may have more than 1 deal.
quota: Total annual quota for each salesperson.
deals Table:

Column Name	Type
employee_id	integer
deal_size	integer
deals Example Input:

employee_id	deal_size
101	400000
101	300000
201	500000
301	500000
sales_quotas Table:

Column Name	Type
employee_id	integer
quota	integer
sales_quotas Example Input:

employee_id	quota
101	500000
201	400000
301	600000
Example Output:

employee_id	made_quota
101	yes
201	yes
301	no
Explanation:

User 101 had $700k in sales, beating their $500k quota. User 201 had $500k in sales, beating their $400k quota. User 301 had $500k in sales, but had a $600k quota, so they didnt hit their goal.

The dataset you are querying against may have different input & output - this is just an example!
'''


SELECT distinct
  a.employee_id,
  case
    when sum(a.deal_size) > b.quota then 'yes'
    else 'no' end as made_quota
  FROM deals a inner join sales_quotas b 
  on a.employee_id = b.employee_id
  group by a.employee_id, b.quota
  order by a.employee_id;



'''
Output
employee_id	made_quota
1	yes
101	yes
201	yes
301	no
405	no
'''
