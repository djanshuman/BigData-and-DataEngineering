"""
https://datalemur.com/questions/user-session-activity
Assume you are given the table containing Twitter user session activities.

Write a query that ranks users according to their total session durations (in minutes) in descending order for each session type between the start date (2022-01-01) and the end date (2022-02-01).

Output the user ID, session type, and the ranking of the total session duration.


session Example Input:
session_id	user_id	session_type	duration	start_date
6368	111	like	3	12/25/2021 12:00:00
1742	111	retweet	6	01/02/2022 12:00:00
8464	222	reply	8	01/16/2022 12:00:00
7153	111	retweet	5	01/28/2022 12:00:00
3252	333	reply	15	01/10/2022 12:00:00
  
Example Output:
user_id	session_type	ranking
333	reply	1
222	reply	2
111	retweet	1
Explanation: User 333 is listed on the top due to the highest duration of 15 minutes. The ranking resets on 3rd row as the session type changes.
"""
with cte as(
SELECT
  user_id,
  session_type,
  sum(duration),
  rank() over (partition by session_type order by sum(duration) desc) as ranking
  from sessions
  where start_date between '01/01/2022 00:00:00' and '02/01/2022 23:59:59'
  group by 1,2)
  SELECT
    user_id,
    session_type,
    ranking from cte;

"""
Output

user_id	session_type	ranking
333	like	1
111	like	2
222	like	3
222	reply	1
333	reply	2
111	retweet	1
"""
