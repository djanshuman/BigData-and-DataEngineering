'''
The Most Expensive Products Per Category
https://platform.stratascratch.com/coding/9607-the-most-expensive-products-per-category?code_type=1

'''

with cte as (
select
    product_category,
    product_name,
    cast(substring(price,2,length(price)) as Decimal) as price,
    rank() over (partition by product_category 
                 order by cast(substring(price,2,length(price)) as Decimal) desc) as rnk
    from innerwear_amazon_com
)select 
        product_category,
        product_name,
        price
        from cte where rnk =1;
