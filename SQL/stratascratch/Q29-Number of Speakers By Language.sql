'''

https://platform.stratascratch.com/coding/10139-number-of-speakers-by-language?code_type=1

Find the number of speakers of each language by country. Output the country, language, and the corresponding number of speakers. Output the result based on the country in ascending order.

'''

select 
    e.location,
    u.language,
    count(distinct u.user_id) as "n_users"
    from
    playbook_events e inner join playbook_users u on e.user_id = u.user_id
    group by e.location,u.language
    order by e.location asc,n_users desc;
