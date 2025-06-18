'''
https://platform.stratascratch.com/coding/9915-highest-cost-orders?code_type=1

Find the customer with the highest daily total order cost between 2019-02-01 to 2019-05-01. 
  If customer had more than one order on a certain day, sum the order costs on daily basis. Output customers first name, total cost of their items, and the date.


For simplicity, you can assume that every first name in the dataset is unique.

'''

with cte as (
select 
    c.first_name,
    o.order_date,
    o.total_order_cost
    from customers c
    inner join orders o
    on c.id = o.cust_id
    where o.order_date between '2019-02-01' and '2019-05-01'),

sum_cte as(
select
    first_name,
    order_date,
    sum(total_order_cost) as "max_cost",
    rank() over(order by sum(total_order_cost) desc) as "rnk"
    from cte group by 1,2)
    
select 
    first_name,
    order_date,
    max_cost
    from sum_cte where rnk=1;

'''
Output
View the output in a separate browser tab
Execution time: 0.00913 seconds

Your Solution:
first_name	order_date	max_cost
Jill	2019-04-19	275
'''
