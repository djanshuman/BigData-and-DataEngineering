'''
https://platform.stratascratch.com/coding/2010-top-streamers?code_type=1
'''

with cte as (
select 
    user_id,
    count(case
            when (session_type= 'streamer') then 1
            else null end ) as streaming,
    count(case
            when (session_type= 'viewer') then 1
            else null end) as view
    from twitch_sessions
group by user_id )
select * from cte where streaming > view;
