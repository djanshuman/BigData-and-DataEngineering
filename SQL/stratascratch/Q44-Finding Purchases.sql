'''
https://platform.stratascratch.com/coding/10553-finding-purchases?code_type=1

Identify returning active users by finding users who made a second purchase within 7 days of any previous purchase. Output a list of these user_ids.

'''

select 
    distinct a.user_id
    from amazon_transactions a inner join amazon_transactions b
    on a.user_id = b.user_id
    where a.id != b.id 
    and b.created_at between a.created_at and a.created_at +  interval '7 day'

'''
Output
View the output in a separate browser tab
Execution time: 0.00896 seconds

Your Solution:
user_id
100
103
105
109
110
111
112
114
117
120
122
128
129
130
131
133
141
143
150
'''
