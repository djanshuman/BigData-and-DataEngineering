'''
https://platform.stratascratch.com/coding/9608-exclusive-amazon-products?code_type=1

Find products which are exclusive to only Amazon and therefore not sold at Top Shop and Macy's. Your output should include the product name, brand name, price, and rating.
Two products are considered equal if they have the same product name and same maximum retail price (mrp column).

'''

select 
    product_name,
    brand_name,
    price,
    rating
    from 
    innerwear_amazon_com
    where (product_name,mrp) not in (
            select distinct 
                product_name , mrp from (
                    select product_name , mrp
                        from innerwear_macys_com
                union all
                    select product_name , mrp
                        from innerwear_topshop_com) 
    h);
