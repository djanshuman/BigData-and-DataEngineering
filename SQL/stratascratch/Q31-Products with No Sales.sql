'''
https://platform.stratascratch.com/coding/2109-products-with-no-sales?code_type=3
Write a query to get a list of products that have not had any sales. Output the ID and market name of these products.

Tables: fct_customer_sales, dim_product

'''

select 
    dp.prod_sku_id,
    dp.market_name
    from dim_product dp left outer join fct_customer_sales cs on dp.prod_sku_id = cs.prod_sku_id
    where cs.prod_sku_id is null;
    
