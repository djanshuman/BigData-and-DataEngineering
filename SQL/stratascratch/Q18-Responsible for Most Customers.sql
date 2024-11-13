'''
https://platform.stratascratch.com/coding/2108-responsible-for-most-customers?code_type=1


Each Employee is assigned one territory and is responsible for the Customers from this territory. There may be multiple employees assigned to the same territory.
Write a query to get the Employees who are responsible for the maximum number of Customers. Output the Employee ID and the number of Customers.

Tables: map_employee_territory, map_customer_territory

map_employee_territory

empl_id	territory_id
E849	T3
E850	T3
E851	T3
E852	T1
E853	T2
E854	T5
E855	T5
E856	T4
E857	T2

map_customer_territory

cust_id	territory_id
C273	T3
C274	T3
C275	T1
C276	T1
C277	T1

  '''



with cte as (
    select
    territory_id,
    count(*) as c_count
    from 
    map_customer_territory
    group by territory_id),
cte1 as (select 
            * from cte
            where c_count = (select max(c_count) from cte))
select 
    empl_id,
    c_count
    from cte1 inner join map_employee_territory on
    cte1.territory_id = map_employee_territory.territory_id;


'''
empl_id	c_count
E849	4
E850	4
E851	4
E856	4
'''
