'''
https://platform.stratascratch.com/coding/10064-highest-energy-consumption?code_type=1
Find the date with the highest total energy consumption from the Meta/Facebook data centers. Output the date along with the total energy consumption across all data centers.
'''


with cte as (
select 
    consumption,date
    from fb_eu_energy 
    union all
select 
    consumption,date
    from fb_asia_energy
    union all
select 
    consumption,date
    from  fb_na_energy),
cte1 as(
    select 
    date,
    sum(consumption) as total_consumption,
    rank() over(order by sum(consumption) desc ) as rnk
    from cte group by date)
    select 
        date,total_consumption
    from cte1 where rnk=1;
