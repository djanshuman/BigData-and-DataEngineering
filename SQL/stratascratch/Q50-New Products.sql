'''
https://platform.stratascratch.com/coding/10318-new-products?code_type=1

Calculate the net change in the number of products launched by companies in 2020 compared to 2019. Your output should include the company names and the net difference.
(Net difference = Number of products launched in 2020 - The number launched in 2019.)


'''

with cte as (
select 
    year,
    company_name,
    count(distinct product_name) as "n_prod_count"
    from car_launches
    group by 1,2)
    
select 
    a.company_name,
    b.n_prod_count - a.n_prod_count as "net_products"
    from cte a inner join cte b
    on a.company_name = b.company_name
    where b.year > a.year

'''
Output
View the output in a separate browser tab
Execution time: 0.01228 seconds

Your Solution:
company_name	net_products
Chevrolet	2
Ford	-1
Honda	-3
Jeep	1
Toyota	-1
'''
