'''
https://platform.stratascratch.com/coding/10140-macbook-pro-events?code_type=1

Find how many events happened on MacBook-Pro per company in Argentina from users that do not speak Spanish.
Output the company id, language of users, and the number of events performed by users.



'''

--solution--

select 
        pu.company_id,
        pu.language,
        count(*) as "n_macbook_pro_events"
        from playbook_events pe inner join playbook_users pu on pe.user_id=pu.user_id
        where lower(pu.language) != 'spanish' AND
        lower(pe.location) = 'argentina' AND
        lower(pe.device) = 'macbook pro'
        group by 1,2;
        
 
