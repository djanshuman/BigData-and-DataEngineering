'''
https://leetcode.com/problems/biggest-window-between-visits/submissions/1628934364/?envType=study-plan-v2&envId=premium-sql-50


SQL Schema
Pandas Schema
Table: UserVisits

+-------------+------+
| Column Name | Type |
+-------------+------+
| user_id     | int  |
| visit_date  | date |
+-------------+------+
This table does not have a primary key, it might contain duplicate rows.
This table contains logs of the dates that users visited a certain retailer.
 

Assume todays date is '2021-1-1'.

Write a solution that will, for each user_id, find out the largest window of days between each visit and the one right after it (or today if you are considering the last visit).

Return the result table ordered by user_id.

The query result format is in the following example.

 
Example 1:

Input: 
UserVisits table:
+---------+------------+
| user_id | visit_date |
+---------+------------+
| 1       | 2020-11-28 |
| 1       | 2020-10-20 |
| 1       | 2020-12-3  |
| 2       | 2020-10-5  |
| 2       | 2020-12-9  |
| 3       | 2020-11-11 |
+---------+------------+
Output: 
+---------+---------------+
| user_id | biggest_window|
+---------+---------------+
| 1       | 39            |
| 2       | 65            |
| 3       | 51            |
+---------+---------------+
Explanation: 
For the first user, the windows in question are between dates:
    - 2020-10-20 and 2020-11-28 with a total of 39 days. 
    - 2020-11-28 and 2020-12-3 with a total of 5 days. 
    - 2020-12-3 and 2021-1-1 with a total of 29 days.
Making the biggest window the one with 39 days.
For the second user, the windows in question are between dates:
    - 2020-10-5 and 2020-12-9 with a total of 65 days.
    - 2020-12-9 and 2021-1-1 with a total of 23 days.
Making the biggest window the one with 65 days.
For the third user, the only window in question is between dates 2020-11-11 and 2021-1-1 with a total of 51 days.
'''
-- Write your PostgreSQL query statement below
with cte as (
select
    user_id,
    visit_date,
    (case 
        when lead(visit_date) over (partition by user_id order by visit_date) is null 
            then to_date('2021-1-1','YYYY-MM-dd')
        else lead(visit_date) over (partition by user_id order by visit_date)
    end) as "next_visit"
    from UserVisits),
    biggest_window_rank as (
    select
        user_id,
        visit_date,
        next_visit,
        (next_visit - visit_date) as biggest_window,
        rank() over(partition by user_id order by (next_visit - visit_date) desc) as rnk
        from cte)
        
    select 
        distinct
        user_id,
        biggest_window
        from biggest_window_rank where rnk=1
        order by 1;

'''
Accepted
Runtime: 134 ms
 
Case 2
Input
UserVisits =
| user_id | visit_date |
| ------- | ---------- |
| 1       | 2020-10-8  |
| 1       | 2020-11-9  |
| 1       | 2020-10-24 |
| 1       | 2020-12-4  |
| 1       | 2020-10-18 |
| 1       | 2020-12-15 |

View more
Output
| user_id | biggest_window |
| ------- | -------------- |
| 1       | 23             |
| 2       | 31             |
| 3       | 38             |
| 4       | 44             |
| 5       | 29             |
| 6       | 17             |
| 7       | 29             |
| 8       | 20             |
| 9       | 32             |

View less
Expected
| user_id | biggest_window |
| ------- | -------------- |
| 1       | 23             |
| 2       | 31             |
| 3       | 38             |
| 4       | 44             |
| 5       | 29             |
| 6       | 17             |
| 7       | 29             |
| 8       | 20             |
| 9       | 32             |
'''
