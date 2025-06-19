'''
https://platform.stratascratch.com/coding/2073-popular-posts?code_type=1

Popular Posts


Last Updated: June 2025

Hard
ID 2073

54

About This Question
The column 'perc_viewed' in the table 'post_views' denotes the percentage of the session duration time the user spent viewing a post. Using it, calculate the total time that each post was viewed by users. Output post ID and the total viewing time in seconds, but only for posts with a total viewing time of over 5 seconds.

'''

select 
    pv.post_id,
    sum(pv.perc_viewed * (s.session_duration/100) ::float) as "total_viewtime"
from post_views pv
inner join 
(select 
    session_id,
    extract(epoch from session_endtime::TIME) - extract(epoch from session_starttime::TIME)
        as "session_duration"
    from user_sessions) s 
    on s.session_id = pv.session_id
    group by 1
    having 
    sum(pv.perc_viewed * (s.session_duration/100) ::float) > 5
    order by 1;

'''
Output
View the output in a separate browser tab
Execution time: 0.00848 seconds

Your Solution:
post_id	total_viewtime
2	24
4	5.1
'''
