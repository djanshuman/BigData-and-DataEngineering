'''
https://platform.stratascratch.com/coding/10538-top-posts-per-channel?code_type=1

'''

with cte as 
(select
channel_name,
post_id,
created_at,
likes,
dense_rank() over(partition by channel_name order by likes desc) as rnk
from posts inner join channels
on posts.channel_id = channels.channel_id
where likes != 0
)select channel_name,
post_id,
created_at,
likes
from cte where rnk <=3;

'''
channel_name	post_id	created_at	likes
DailyUpdates	134	2024-08-16	32
DailyUpdates	139	2024-07-15	30
DailyUpdates	138	2024-08-11	6
GameStream	111	2024-07-26	39
GameStream	118	2024-08-24	34
GameStream	112	2024-08-08	14
ProGamer	145	2024-08-06	49
ProGamer	148	2024-10-05	28
ProGamer	150	2024-08-24	26
SocialBuzz	129	2024-08-24	40
SocialBuzz	127	2024-09-30	9
SocialBuzz	123	2024-07-28	9
TechNews	105	2024-09-07	38
TechNews	103	2024-07-24	11
TechNews	104	2024-09-13	3

'''
