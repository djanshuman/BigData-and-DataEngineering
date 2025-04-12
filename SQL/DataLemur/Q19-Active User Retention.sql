"""

"""
SELECT
  extract(month from event_date) as "mth",
  count(distinct user_id) as "monthly_active_users"
  from user_actions as current_month
  where exists(
              select * from user_actions as "prev_month"
              where current_month.user_id = prev_month.user_id
              and extract(month from prev_month.event_date) 
              = extract(month from current_month.event_date - interval '1 month')
        )
  and event_type in ('sign-in', 'like','comment')
  and extract(month from event_date) = '7'
  and extract(year from event_date) = '2022'
  group by 1;

"""
Output

mth	monthly_active_users
7	2
"""
