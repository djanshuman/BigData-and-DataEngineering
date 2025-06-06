'''
https://leetcode.com/problems/find-the-start-and-end-number-of-continuous-ranges/description/?envType=study-plan-v2&envId=premium-sql-50

SQL Schema
Pandas Schema
Table: Logs

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| log_id        | int     |
+---------------+---------+
log_id is the column of unique values for this table.
Each row of this table contains the ID in a log Table.
 

Write a solution to find the start and end number of continuous ranges in the table Logs.

Return the result table ordered by start_id.

The result format is in the following example.

 

Example 1:

Input: 
Logs table:
+------------+
| log_id     |
+------------+
| 1          |
| 2          |
| 3          |
| 7          |
| 8          |
| 10         |
+------------+
Output: 
+------------+--------------+
| start_id   | end_id       |
+------------+--------------+
| 1          | 3            |
| 7          | 8            |
| 10         | 10           |
+------------+--------------+
Explanation: 
The result table should contain all ranges in table Logs.
From 1 to 3 is contained in the table.
From 4 to 6 is missing in the table
From 7 to 8 is contained in the table.
Number 9 is missing from the table.
Number 10 is contained in the table.
'''

-- Write your PostgreSQL query statement below
with cte as (
select
   log_id,
   log_id - row_number() over(order by log_id) as rnk
   from logs)
   select
    min(log_id) as start_id , 
    max(log_id) as end_id
    from cte
    group by rnk
    order by 1,2;
'''
Accepted
Runtime: 147 ms
Case 1
Input
Logs =
| log_id |
| ------ |
| 1      |
| 2      |
| 3      |
| 7      |
| 8      |
| 10     |
Output
| start_id | end_id |
| -------- | ------ |
| 1        | 3      |
| 7        | 8      |
| 10       | 10     |
Expected
| start_id | end_id |
| -------- | ------ |
| 1        | 3      |
| 7        | 8      |
| 10       | 10     |
'''
