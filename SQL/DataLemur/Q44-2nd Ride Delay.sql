"""
As a data analyst at Uber, it's your job to report the latest metrics for specific groups of Uber users. Some riders create their Uber account the same day they book their first ride; the rider engagement team calls them "in-the-moment" users.

Uber wants to know the average delay between the day of user sign-up and the day of their 2nd ride. Write a query to pull the average 2nd ride delay for "in-the-moment" Uber users. Round the answer to 2-decimal places.

Tip:

You don't need to use date operations to get the answer! You're welcome to, but it's not necessary.
users Table:
Column Name	Type
user_id	integer
registration_date	date
users Example Input:
user_id	registration_date
1	08/15/2022
2	08/21/2022
rides Table:
Column Name	Type
ride_id	integer
user_id	integer
ride_date	date
rides Example Input:
ride_id	user_id	ride_date
1	1	08/15/2022
2	1	08/16/2022
3	2	09/20/2022
4	2	09/23/2022
Example Output:
average_delay
1
Explanation:
We do not include user 2 in the calculation because the user was not created on the same date as the first ride.
For user 1, the difference between the first and second rides was 1 day; thus, the overall average is 1 day.
The dataset you are querying against may have different input & output - this is just an example!
"""
WITH MOMENT_USERS AS (
  SELECT DISTINCT users.user_id
  FROM users 
  INNER JOIN rides
    ON users.user_id = rides.user_id
    AND users.registration_date = rides.ride_date
    ),
-- row_number() is used instead of rank() and dense_rank() as some user has taken second ride on same day as regd date
-- rank and dense_rank() will assign rank=1 for all even 2nd ride as 2nd ride date is same as regd date and we are doing order by ride_date in window func.
-- so rank=2 won't be there for user =5 and it will be skipped.
  rider_calc as (
  SELECT
    MU.user_id,
    R.ride_date as "second_ride",
    row_number() over (partition by MU.user_id order by R.ride_date) as "rn", 
    LAG(R.ride_date) OVER(PARTITION BY MU.user_id order by R.ride_date) AS "first_ride"
    FROM 
    MOMENT_USERS MU LEFT OUTER JOIN rides R 
    ON MU.user_id = R.user_id)
    
    SELECT
      round(avg(second_ride::date - first_ride::date),2) as "average_delay"
      from rider_calc
      where rn=2;

"""
Output

average_delay
0.67
"""
