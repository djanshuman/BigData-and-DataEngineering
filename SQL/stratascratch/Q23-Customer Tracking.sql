'''
  https://platform.stratascratch.com/coding/2136-customer-tracking?code_type=1

Given the users sessions logs on a particular day, calculate how many hours each user was active that day.


Note: The session starts when state=1 and ends when state=0.

cust_id	state	timestamp
c001	1	07:00:00
c001	0	09:30:00
c001	1	12:00:00
c001	0	14:30:00
c002	1	08:00:00
c002	0	09:30:00
c002	1	11:00:00
c002	0	12:30:00
c002	1	15:00:00
c002	0	16:30:00
c003	1	09:00:00
c003	0	10:30:00
c004	1	10:00:00
c004	0	10:30:00
c004	1	14:00:00
c004	0	15:30:00
c005	1	10:00:00
c005	0	14:30:00
c005	1	15:30:00
c005	0	18:30:00
'''
with time_between as (
select 
    cust_id,
    state,
    timestamp,
    LAG(timestamp,-1) over(PARTITION BY cust_id ORDER BY timestamp) as next_event,
    extract (epoch from cast (LAG(timestamp,-1) over(PARTITION BY cust_id 
                    ORDER BY timestamp) as time) - cast(timestamp as time))/3600 AS hours_to_next_event

from cust_tracking)
select
    cust_id,
    sum(hours_to_next_event)
    from time_between
    where state =1
    group by cust_id;


'''
cust_id	sum
c001	5
c002	4.5
c003	1.5
c004	2
c005	7.5
'''
