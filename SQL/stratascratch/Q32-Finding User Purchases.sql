'''
https://platform.stratascratch.com/coding/10322-finding-user-purchases?code_type=1
Identify returning active users by finding users who made a second purchase within 7 days of any previous purchase. Output a list of these user_ids.

'''

select 
    distinct a.user_id
    from amazon_transactions a inner join amazon_transactions b
    on a.user_id= b.user_id where a.id <> b.id
    and b.created_at between a.created_at and a.created_at + interval '7 day';
