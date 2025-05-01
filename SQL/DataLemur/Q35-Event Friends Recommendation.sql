"""
https://datalemur.com/questions/event-friends-rec

Facebook wants to recommend new friends to people who show interest in attending 2 or more of the same private events.

Sort your results in order of user_a_id and user_b_id (refer to the Example Output below).

Notes:

A user interested in attending would have either 'going' or 'maybe' as their attendance status.
Friend recommendations are unidirectional, meaning if user x and user y should be recommended to each other, the result table should have both user x recommended to user y and user y recommended to user x.
The result should not contain duplicates (i.e., user y should not be recommended to user x multiple times).
friendship_status Table:
Column Name	Type
user_a_id	integer
user_b_id	integer
status	enum ('friends', 'not_friends')
Each row of this table indicates the status of the friendship between user_a_id and user_b_id.

friendship_status Example Input:
user_a_id	user_b_id	status
111	333	not_friends
222	333	not_friends
333	222	not_friends
222	111	friends
111	222	friends
333	111	not_friends
event_rsvp Table:
Column Name	Type
user_id	integer
event_id	integer
event_type	enum ('public', 'private')
attendance_status	enum ('going', 'maybe', 'not_going')
event_date	date
  
event_rsvp Example Input:
user_id	event_id	event_type	attendance_status	event_date
111	567	public	going	07/12/2022
222	789	private	going	07/15/2022
333	789	private	maybe	07/15/2022
111	234	private	not_going	07/18/2022
222	234	private	going	07/18/2022
333	234	private	going	07/18/2022
  
Example Output:
user_a_id	user_b_id
222	333
333	222
Users 222 and 333 who are not friends have shown interest in attending 2 or more of the same private events.

The dataset you are querying against may have different input & output - this is just an example!
"""

--Event Friends Recommendation

-- Solution 1

with cte as (
select
  ev1.user_id as user_a_id,
  ev2.user_id as user_b_id
  from event_rsvp ev1 inner join event_rsvp ev2
  on ev1.user_id != ev2.user_id
  where ev1.event_id = ev2.event_id
  and ev1.event_type = ev2.event_type
  and ev1.event_type = 'private' and ev2.event_type = 'private'
  and ev1.attendance_status in ('maybe','going')
  and ev2.attendance_status in ('maybe','going')
  group by 1,2
  having count(*) >= 2)
  SELECT
  distinct 
    ct1.user_a_id,
    ct2.user_b_id
     from cte as ct1
    inner join friendship_status ct2
    on ct1.user_a_id = ct2.user_a_id
    where ct1.user_b_id = ct2.user_b_id
    and status = 'not_friends';
    
    
-- Solution 2:

with private_events as (
SELECT
  * from 
  event_rsvp
  where event_type = 'private'
  and attendance_status in ('going','maybe'))
SELECT
  a.user_id as user_a_id,
  b.user_id as user_b_id
  from private_events a inner join private_events b 
  on a.user_id != b.user_id
  and a.event_id = b.event_id
  inner join friendship_status f
  on a.user_id = f.user_a_id
  where b.user_id = f.user_b_id
  and f.status = 'not_friends'
  group by 1,2
  having count(*) >= 2
  order by 1,2;

"""
Output

user_a_id	user_b_id
222	333
333	222
333	444
444	333

"""
