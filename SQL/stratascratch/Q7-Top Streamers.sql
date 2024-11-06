'''
https://platform.stratascratch.com/coding/2010-top-streamers?code_type=1

Top Streamers

List the top 10 users who accumulated the most sessions where they had more streaming sessions than viewing. Return the user_id, number of streaming sessions, and number of viewing sessions.

Table: twitch_sessions
Hints
Expected Output
twitch_sessions
Preview
user_id	session_start	session_end	session_id	session_type
0	2020-08-11 05:51:31	2020-08-11 05:54:45	539	streamer
2	2020-07-11 03:36:54	2020-07-11 03:37:08	840	streamer
3	2020-11-26 11:41:47	2020-11-26 11:52:01	848	streamer
1	2020-11-19 06:24:24	2020-11-19 07:24:38	515	viewer
2	2020-11-14 03:36:05	2020-11-14 03:39:19	646	viewer
0	2020-03-11 03:01:40	2020-03-11 03:01:59	782	streamer
0	2020-08-11 03:50:45	2020-08-11 03:55:59	815	viewer
3	2020-10-11 22:15:14	2020-10-11 22:18:28	630	viewer
1	2020-11-20 06:59:57	2020-11-20 07:20:11	907	streamer
2	2020-07-11 14:32:19	2020-07-11 14:42:33	949	viewer
user_id:
int
session_start:
datetime
session_end:
datetime
session_id:
int
session_type:
varchar
'''
with cte 
    as (
        select * from 
            (select 
                 user_id,
                count(case
                        when (session_type= 'streamer') then 1
                        else null end ) as streaming,
                count(case
                        when (session_type= 'viewer') then 1
                        else null end) as view
                        from twitch_sessions
                        group by user_id) 
            a where a.streaming > a.view),
CTE_final 
    AS (select
        user_id,
        streaming,
        view,
        rank() over (order by streaming+view desc) as rnk 
        from cte
        )
            select 
            user_id,
            streaming,
            view
            from CTE_final where rnk <=10;
'''
Output
View the output in a separate browser tab
Execution time: 0.00734 seconds

user_id	streaming	view
0	2	1
 '''
