'''
https://platform.stratascratch.com/coding/10352-users-by-avg-session-time?code_type=1

Calculate each user's average session time, where a session is defined as the time difference between a page_load and a page_exit. Assume each user has only one session per day. If there are multiple page_load or page_exit events on the same day,
use only the latest page_load and the earliest page_exit, ensuring the page_load occurs before the page_exit. Output the user_id and their average session time.

'''
--solution--
with events as (
SELECT
    user_id,
    timestamp,
    action,
    row_number() over(partition by user_id order by timestamp) as "seq"
    from facebook_web_log 
    where action in ('page_load','page_exit')),
matching_events as (
    select
        a.user_id,
        a.timestamp as "load_time",
        b.timestamp as "exit_time",
        b.timestamp - a.timestamp as "session_time"
        from events a inner join events b on 
        a.user_id = b.user_id and a.seq = b.seq -1
        and a.action = 'page_load' and b.action = 'page_exit')
    select 
        user_id,
        avg(session_time) as "avg_session_time"
        from matching_events
        group by user_id;
