"""
https://datalemur.com/questions/booking-referral-source
The Airbnb marketing analytics team is trying to understand what are the most common marketing channels that lead users to book their first rental on Airbnb.

Write a query to find the top marketing channel and percentage of first rental bookings from the aforementioned marketing channel. Round the percentage to the closest integer. Assume there are no ties.

Assumptions:

Marketing channel with null values should be incorporated in the percentage of first bookings calculation, but the top channel should not be a null value. Meaning, we cannot have null as the top marketing channel.
To avoid integer division, multiple the percentage with 100.0 and not 100.
bookings Table:
Column Name	Type
booking_id	integer
user_id	integer
booking_date	datetime
bookings Example Input:
booking_id	user_id	booking_date
1	1	01/01/2022 00:00:00
2	1	01/06/2022 00:00:00
6	2	01/06/2022 00:00:00
8	3	01/06/2022 00:00:00
booking_attribution Table:
Column Name	Type
booking_id	integer
channel	string
booking_attribution Example Input:
booking_id	channel
1	organic search
2	
3	organic search
4	referral
5	email
6	organic search
7	paid search
8	
9	paid search
10	paid search
Example Output:
channel	first_booking_pct
organic search	67
Explanation:
We know that user 1's first booking was organic search, user 2's was organic search, and user 3's was null. Thus, 2 bookings via organic search / 3 total bookings = 67%.

The dataset you are querying against may have different input & output - this is just an example!
  
"""
with master_booking as (
SELECT
  user_id,
  booking_id,
  booking_date,
  rank() over (partition by user_id order by booking_date) as "rnk"
  from bookings
  ),
  
  top_booking as (
  SELECT
    distinct
    ba.channel,
    round(((100.0 * count(*) over(partition by channel)) / count(*) over()),0) as "first_booking_pct"
    from master_booking mb inner join booking_attribution ba 
    on mb.booking_id = ba.booking_id
    where mb.rnk=1)
    
    SELECT
      channel,
      first_booking_pct
      from top_booking where first_booking_pct =
        (select max(first_booking_pct) from top_booking
          where channel is not null)
"""
Output

channel	first_booking_pct
direct	40
"""
