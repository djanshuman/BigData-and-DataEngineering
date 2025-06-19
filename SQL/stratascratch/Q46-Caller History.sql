'''
https://platform.stratascratch.com/coding/2132-caller-history?code_type=1

Given a phone log table that has information about callers call history, find out the callers whose first and last calls were to the same person on a given day. Output the caller ID, recipient ID, and the date called.


'''

with cte as (
select 
    a.*,
    first_value(a.recipient_id) over(partition by a.caller_id,
            to_char(date_called::date , 'YYYY-MM-dd')
            order by date_called) as "f_recp",
    first_value(a.recipient_id) over(partition by a.caller_id,
            to_char(date_called::date , 'YYYY-MM-dd')
            order by date_called desc ) as "l_recp"
    from caller_history a)

select 
    distinct
    caller_id,
    f_recp as "recipient",
    to_char(date_called::date , 'YYYY-MM-dd') as "date"
    from cte
    where f_recp = l_recp;


-- brute force way --

with cte as (
select
    caller_id,
    to_char(date_called::date , 'YYYY-MM-dd') as "date",
    min(date_called) as "first_call",
    max(date_called) as "last_call"
    from caller_history
    group by 1,2),

finding_match as (   
select
    a.caller_id,
    b.recipient_id as "first_call_recp",
    c.recipient_id as "last_call_recp",
    a.date
    from cte a inner join caller_history b
    on a.caller_id = b.caller_id
    and a.first_call = b.date_called
    inner join caller_history c
    on a.caller_id = c.caller_id
    and a.last_call = c.date_called)
    
select 
    caller_id,
    first_call_recp as "recipient_id",
    date
    from finding_match
    where first_call_recp = last_call_recp;
    

'''
Output
View the output in a separate browser tab
Execution time: 0.01197 seconds

Your Solution:
caller_id	recipient	date
2	3	2022-08-01
2	4	2022-08-02
2	5	2022-07-06
'''
