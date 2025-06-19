'''
https://platform.stratascratch.com/coding/10322-finding-user-purchases?code_type=1
Identify returning active users by finding users who made a second purchase within 7 days of any previous purchase. Output a list of these user_ids.

'''


with f_purchase as (
select
    user_id,
    min(created_at) as "first_purchase"
    from amazon_transactions
    group by 1)
select
    distinct a.user_id
    from f_purchase a inner join amazon_transactions b
    on a.user_id = b.user_id
    where b.created_at > a.first_purchase
    and b.created_at between a.first_purchase and a.first_purchase + interval '7 day'
    order by 1;

'''Output
View the output in a separate browser tab
Execution time: 0.00918 seconds

Your Solution:
user_id
100
103
105
109
111
114
117
122
130
131
133
141
143
'''
