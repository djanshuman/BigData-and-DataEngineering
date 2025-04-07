"""
https://datalemur.com/questions/frequently-purchased-pairs

Given the Walmart transaction and product tables, write a query to determine the count of unique product combinations that are purchased together in the same transaction, considering that there must be a minimum of two products in the transaction. Display the output in ascending order of the product combinations.

For instance, if there are two transactions where apples and bananas are bought together, and another transaction where bananas and soy milk are bought together, the total count of unique combinations would be 2.

Psst, you may or may not need to use the products table.

Effective April 17th, 2023, the problem statement, assumptions, and solution were modified to align with the question.

transactions Table:
transactions Example Input:
transaction_id	product_id	user_id	transaction_date
231574	111	234	03/01/2022 12:00:00
231574	444	234	03/01/2022 12:00:00
231574	222	234	03/01/2022 12:00:00
137124	111	125	03/05/2022 12:00:00

products Example Input:
product_id	product_name
111	apple
222	soy milk
333	instant oatmeal
444	banana
555	chia seed
  
Example Output:
combination
111","222","444
  
"""

with array_table as (
select 
  transaction_id,
  array_agg(cast(product_id as text) order by product_id) as "combinations"
  from transactions
  group by transaction_id)
  SELECT
    distinct combinations
    from array_table
    where array_length(combinations,1) > 1
    order by combinations;
