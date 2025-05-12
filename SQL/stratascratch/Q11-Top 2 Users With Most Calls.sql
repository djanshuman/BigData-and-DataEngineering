'''
https://platform.stratascratch.com/coding/2019-top-2-users-with-most-calls?code_type=1

  Top 2 Users With Most Calls

Return the top 2 users in each company that called the most. Output the company_id, user_id, and the users rank. If there are multiple users in the same rank, keep all of them.

Tables: rc_calls, rc_users

rc_calls

user_id	date	call_id
1218	2020-04-19 01:06:00	0
1554	2020-03-01 16:51:00	1
1857	2020-03-29 07:06:00	2
1525	2020-03-07 02:01:00	3
1271	2020-04-28 21:39:00	4

rc_users

user_id	status	company_id
1218	free	1
1554	inactive	1
1857	free	2
1525	paid	1
1271	inactive	2
1181	inactive	2
1950	free	1
1339	free	2
1910	free	2
1093	paid	3
1859	free	1
1079	paid	2
1519	inactive	2
1854	paid	1
1968	inactive	2
1891	paid	2
1575	free	2
1162	paid	2
1503	inactive	3
1884	free	1
user_id:

'''

select 
company_id,
user_id,
rank
from (select 
    rc_users.company_id,
    rc_calls.user_id,
    dense_rank() over(partition by rc_users.company_id order by count(rc_calls.call_id) desc) as rank
from rc_calls inner join rc_users on rc_users.user_id= rc_calls.user_id
group by 1,2) s
where rank <=2 order by company_id;


'''
Your Solution:
company_id	user_id	rank
1	1859	1
1	1854	2
1	1525	2
2	1181	1
2	1891	1
2	1162	1
2	1910	2
2	1857	2
3	1503	1
3	1093	1
'''
