'''
https://platform.stratascratch.com/coding/2148-monthly-sales-rolling-average?code_type=1
You have been asked to calculate the rolling average for book sales in 2022.


A rolling average continuously updates a data sets average to include all data in the set up to that point. For example, the rolling average for February would be calculated by adding the book sales from January and February and dividing by two; the rolling average for March would be calculated by adding the book sales from January, February, and March and dividing by three; and so on.


Output the month, the sales for that month, and an extra column containing the rolling average rounded to the nearest whole number.
'''
with cte as (
select 
    extract (month from b.order_date) as "order_month",
    sum(b.quantity * a.unit_price) as "sales"
    from amazon_books a inner join book_orders b
    on a.book_id = b.book_id
    where extract (year from b.order_date) = '2022'
    group by 1)
    
    select 
        order_month,
        sales,
        round(avg(sales) over (
        order by order_month
        rows between unbounded preceding and current row),0) as "rolling_average"
        from cte
        order by 1;

'''
Output
View the output in a separate browser tab
Execution time: 0.00853 seconds

Your Solution:
order_month	sales	rolling_average
1	145	145
2	250	198
3	315	237
4	400	278
5	450	312
6	315	313
7	465	334
8	540	360
9	315	355
10	350	355
11	575	375
12	710	403
'''
