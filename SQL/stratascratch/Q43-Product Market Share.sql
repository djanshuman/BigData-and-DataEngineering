'''
https://platform.stratascratch.com/coding/2112-product-market-share?code_type=1

Product Market Share


Last Updated: May 2025

Hard

About This Question
Write a query to find the Market Share at the Product Brand level for each Territory, for Time Period Q4-2021. Market Share is the number of Products of a certain Product Brand brand sold in a territory, divided by the total number of Products sold in this Territory.
Output the ID of the Territory, name of the Product Brand and the corresponding Market Share in percentages. Only include these Product Brands that had at least one sale in a given territory.
'''

with cte as (
select
    mc.territory_id,
    dm.prod_brand,
    count(*) as "n_product_count"
    from fct_customer_sales fc
    inner join map_customer_territory mc
    on fc.cust_id = mc.cust_id
    inner join dim_product dm
    on dm.prod_sku_id = fc.prod_sku_id
    where extract(year from order_date) = '2021'
    and extract(quarter from order_date) = 4
    group by 1,2)
select
    territory_id,
    prod_brand,
    n_product_count / sum(n_product_count) over (partition by territory_id) * 100 as "market_share"
    from cte;

'''
Your Solution:
territory_id	prod_brand	market_share
T1	Apple	33.33
T1	JBL	16.67
T1	Samsung	50
T2	Apple	25
T2	Samsung	75
T3	Apple	37.5
T3	Canon	12.5
T3	Dell	12.5
T3	GoPro	25
T3	JBL	12.5
T4	Apple	41.67
T4	Dell	8.33
T4	GoPro	8.33
T4	Samsung	41.67
T5	Apple	27.27
T5	Dell	9.09
T5	GoPro	27.27
T5	JBL	18.18
T5	Samsung	18.18
'''
