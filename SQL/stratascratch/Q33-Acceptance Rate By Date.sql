'''
https://platform.stratascratch.com/coding/10285-acceptance-rate-by-date?code_type=1

Calculate the friend acceptance rate for each date when friend requests were sent. A request is sent if action = sent and accepted if action = accepted. If a request is not accepted, there is no record of it being accepted in the table. The output will only include dates where requests were sent and at least one of them was accepted, 
  as the acceptance rate can only be calculated for those dates. Show the results ordered from the earliest to the latest date.
'''

with sent_cte as (
    select
        date,
        user_id_sender,
        user_id_receiver
        from fb_friend_requests where action ='sent'),
receiver_cte as (
    select
        date,
        user_id_sender,
        user_id_receiver
        from fb_friend_requests where action ='accepted'
    )
    select
        s.date,
        count(r.user_id_receiver)/cast(count(s.user_id_sender) as decimal) as "percentage_acceptance"
        from sent_cte s left outer join receiver_cte r
        on s.user_id_sender = r.user_id_sender
        and s.user_id_receiver = r.user_id_receiver
        group by s.date;
        
        
        
