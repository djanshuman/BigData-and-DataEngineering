'''

https://platform.stratascratch.com/coding/10319-monthly-percentage-difference?code_type=1
  
Given a table of purchases by date, calculate the month-over-month percentage change in revenue. The output should include the year-month date (YYYY-MM) and percentage change, 
rounded to the 2nd decimal point, and sorted from the beginning of the year to the end of the year.
The percentage change column will be populated from the 2nd month forward and can be calculated as ((this month's revenue - last months revenue) / last month's revenue)*100.
'''


with cte as(
select
    to_char(created_at::date,'YYYY-MM') as year_month,
    sum(value) as "current_mon_revenue",
    lag(sum(value)) over (order by to_char(created_at::date,'YYYY-MM')) as "previous_month_revenue" 
    from sf_transactions
    group by to_char(created_at::date,'YYYY-MM'))
    select 
        year_month,
        round((((current_mon_revenue - previous_month_revenue) / previous_month_revenue)*100),2) as revenue_diff_pct
        from cte;
