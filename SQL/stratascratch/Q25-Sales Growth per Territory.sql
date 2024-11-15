'''
https://platform.stratascratch.com/coding/2111-sales-growth-per-territory?code_type=1

Write a query to return Territory and corresponding Sales Growth. Compare growth between periods Q4-2021 vs Q3-2021.
If Territory (say T123) has Sales worth $100 in Q3-2021 and Sales worth $110 in Q4-2021, then the Sales Growth will be 10% [ i.e. = ((110 - 100)/100) * 100 ]
Output the ID of the Territory and the Sales Growth. Only output these territories that had any sales in both quarters.

Tables: fct_customer_sales, map_customer_territory

fct_customer_sales

cust_id	prod_sku_id	order_date	order_value	order_id
C274	P474	2021-06-28	1500	O110
C285	P472	2021-06-28	899	O118
C282	P487	2021-06-30	500	O125
C282	P476	2021-07-02	999	O146
C284	P487	2021-07-07	500	O149

cust_id	territory_id
C273	T3
C274	T3
C275	T1
C276	T1
C277	T1
C278	T2
C279	T2
C280	T4
C281	T4
C282	T4
C283	T4
C284	T5
C285	T5
C286	T3
C287	T3
'''

--solution-1--
with sales_by_quarter_territory as 
(select 
    territory_id,
    extract(quarter from order_date) as quarter,
    sum(order_value) as total_order_value
    from fct_customer_sales fcs
    inner join map_customer_territory mct
    on fcs.cust_id = mct.cust_id
    where extract(year 
                    from order_date) = 2021
    and extract(quarter from order_date) in (3,4)
    group by 1,2)
    select
        a.territory_id,
        ((b.total_order_value - a.total_order_value) /a.total_order_value) * 100 as sales_growth
        from sales_by_quarter_territory a
        inner join sales_by_quarter_territory b
        on a.territory_id = b.territory_id
        and a.quarter < b.quarter;

--solution-2--

with cte04 as (
select 
    mct.territory_id,
    sum(fcs.order_value) as sales_04
    from fct_customer_sales fcs
    inner join map_customer_territory mct
    on fcs.cust_id = mct.cust_id
    where
    extract(year from order_date) = '2021'
    and extract(quarter from order_date) = '4'
    group by 1),
cte03 as (
    select 
    mct.territory_id,
    sum(fcs.order_value) as sales_03
    from fct_customer_sales fcs
    inner join map_customer_territory mct
    on fcs.cust_id = mct.cust_id
    where
    extract(year from order_date) = '2021'
    and extract(quarter from order_date) = '3'
    group by 1
)
select
    cte03.territory_id,
    ((sales_04 - sales_03) / sales_03) *100 as sales_growth
    from cte03 inner join cte04 
    on cte03.territory_id = cte04.territory_id

'''
territory_id	sales_growth
T1	4.44
T3	67.12
T4	17.36
T5	6.07
'''
