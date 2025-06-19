'''
https://platform.stratascratch.com/coding/2090-first-day-retention-rate?code_type=1

First Day Retention Rate


Last Updated: June 2025

Hard
ID 2090

60

About This Question
Calculate the first-day retention rate of a group of video game players. The first-day retention occurs when a player logs in 1 day after their first-ever log-in.
Return the proportion of players who meet this definition divided by the total number of players.
'''
with f_login_date
as (select 
    player_id,
    min(login_date) as "first_login_date"
    from players_logins
    group by 1)
    
select 
    cast(count(distinct player_id) as float)/(select count(distinct player_id) from players_logins) as "retention_rate" from (
    select
    p.player_id,
    f.first_login_date,
    p.login_date as "second_login_date"
    from players_logins p inner join f_login_date f
    on p.player_id = f.player_id
    where p.login_date > f.first_login_date
    and datediff(p.login_date,f.first_login_date) = 1) a;

'''
Your Solution:
retention_rate
0.5
'''
