"""
https://datalemur.com/questions/fill-missing-product

When accessing Accenture's retailer client's database, you observe that the category column in products table contains null values.

Write a query that returns the updated product table with all the category values filled in, taking into consideration the assumption that the first product in each category will always have a defined category value.

Assumptions:

Each category is expected to be listed only once in the column and products within the same category should be grouped together based on sequential product IDs.
The first product in each category will always have a defined category value.
For instance, the category for product ID 1 is 'Shoes', then the subsequent product IDs 2 and 3 will be categorised as 'Shoes'.
Similarly, product ID 4 is 'Jeans', then the following product ID 5 is categorised as 'Jeans' category, and so forth.

"""

with filled_category as (
select
  *,
 count(category) over(order by product_id) as numbered_category
 from products)
 select
  product_id,
  coalesce(category,max(category) over(partition by numbered_category)) as "category",
  name
  from filled_category;
