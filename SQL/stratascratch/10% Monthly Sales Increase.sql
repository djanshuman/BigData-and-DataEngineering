'''
https://platform.stratascratch.com/coding/2157-10-monthly-sales-increase?code_type=1

You have been asked to compare sales of the current month, May, to those of the previous month, April.


The company requested that you only display products whose sales (UNITS SOLD * PRICE) have increased by more than 10% from the previous month to the current month.


Your output should include the product id and the percentage growth in sales.
'''



with cte as (
select 
    extract(month from date_sold) as "month",
    product_id,
    sum(units_sold * cost_in_dollars) as "total_sales"
    from online_orders
    where extract(month from date_sold) in (4,5)
    group by 1,2)

select 
    a.product_id,
    (b.total_sales - a.total_sales)/a.total_sales *100 as "pc_growth"
    from cte a inner join cte b on a.product_id = b.product_id
    where b.month > a.month
    and (b.total_sales - a.total_sales)/a.total_sales *100 > 10
    and b.month = 5;
    

'''
Output
View the output in a separate browser tab
Execution time: 0.00756 seconds

Your Solution:
product_id	pc_growth
3	135
5	14.29
10	37.04
  
'''
