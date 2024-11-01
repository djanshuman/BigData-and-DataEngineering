'''
https://platform.stratascratch.com/coding/2010-top-streamers?code_type=1
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
