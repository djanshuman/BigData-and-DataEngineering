'''
https://platform.stratascratch.com/coding/2112-product-market-share?code_type=1

Write a query to find the Market Share at the Product Brand level for each Territory, for Time Period Q4-2021. Market Share is the number of Products of a certain Product Brand brand sold in a territory, divided by the total number of Products sold in this Territory.
Output the ID of the Territory, name of the Product Brand and the corresponding Market Share in percentages. Only include these Product Brands that had at least one sale in a given territory.

Tables: fct_customer_sales, map_customer_territory, dim_product

fct_customer_sales

cust_id	prod_sku_id	order_date	order_value	order_id
C274	P474	2021-06-28	1500	O110
C285	P472	2021-06-28	899	O118
C282	P487	2021-06-30	500	O125
C282	P476	2021-07-02	999	O146

map_customer_territory

cust_id	territory_id
C273	T3
C274	T3
C275	T1
C276	T1
C277	T1
C278	T2

dim_product

prod_sku_id	prod_sku_name	prod_brand	market_name
P472	iphone-13	Apple	Apple IPhone 13
P473	iphone-13-promax	Apple	Apply IPhone 13 Pro Max
P474	macbook-pro-13	Apple	Apple Macbook Pro 13''
P475	macbook-air-13	Apple	Apple Makbook Air 13''
P476	ipad	Apple	Apple IPad
'''

with cte_brand_ter as(select
    dp.prod_brand,
    mct.territory_id,
    count(*) as n_sales
    from fct_customer_sales fcs
    inner join map_customer_territory mct
    on fcs.cust_id = mct.cust_id
    inner join dim_product dp
    on dp.prod_sku_id = fcs.prod_sku_id
    where extract(year from order_date) = '2021'
        and extract(quarter from order_date) = '4'
    group by 1,2)
        select 
            prod_brand,
            territory_id,
            (n_sales / sum(n_sales) over (partition by territory_id )) * 100 as market_share
            from cte_brand_ter;

'''
Your Solution:
prod_brand	territory_id	market_share
JBL	T1	16.67
Apple	T1	33.33
Samsung	T1	50
Apple	T2	25
Samsung	T2	75
GoPro	T3	25
Apple	T3	37.5
Canon	T3	12.5
Dell	T3	12.5
JBL	T3	12.5
Dell	T4	8.33
Apple	T4	41.67
Samsung	T4	41.67
GoPro	T4	8.33
Apple	T5	27.27
Dell	T5	9.09
GoPro	T5	27.27
Samsung	T5	18.18
JBL	T5	18.18

'''
