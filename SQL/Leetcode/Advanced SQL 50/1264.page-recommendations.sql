'''
https://leetcode.com/problems/page-recommendations/description/
Table: Friendship

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| user1_id      | int     |
| user2_id      | int     |
+---------------+---------+
(user1_id, user2_id) is the primary key (combination of columns with unique values) for this table.
Each row of this table indicates that there is a friendship relation between user1_id and user2_id.
 

Table: Likes

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| user_id     | int     |
| page_id     | int     |
+-------------+---------+
(user_id, page_id) is the primary key (combination of columns with unique values) for this table.
Each row of this table indicates that user_id likes page_id.
 

Write a solution to recommend pages to the user with user_id = 1 using the pages that your friends liked. It should not recommend pages you already liked.

Return result table in any order without duplicates.

The result format is in the following example.

 

Example 1:

Input: 
Friendship table:
+----------+----------+
| user1_id | user2_id |
+----------+----------+
| 1        | 2        |
| 1        | 3        |
| 1        | 4        |
| 2        | 3        |
| 2        | 4        |
| 2        | 5        |
| 6        | 1        |
+----------+----------+
Likes table:
+---------+---------+
| user_id | page_id |
+---------+---------+
| 1       | 88      |
| 2       | 23      |
| 3       | 24      |
| 4       | 56      |
| 5       | 11      |
| 6       | 33      |
| 2       | 77      |
| 3       | 77      |
| 6       | 88      |
+---------+---------+
Output: 
+------------------+
| recommended_page |
+------------------+
| 23               |
| 24               |
| 56               |
| 33               |
| 77               |
+------------------+
Explanation: 
User one is friend with users 2, 3, 4 and 6.
Suggested pages are 23 from user 2, 24 from user 3, 56 from user 3 and 33 from user 6.
Page 77 is suggested from both user 2 and user 3.
Page 88 is not suggested because user 1 already likes it.
'''
-- Write your PostgreSQL query statement below
with friendship as (
select
    user1_id as user_id , user2_id as friend_id
    from Friendship
union 
select
    user2_id as user_id , user1_id as friend_id
    from Friendship),

liked_pages as (
select 
    f.user_id,
    f.friend_id,
    l.page_id as "friend_liked_page_id"
    from Likes l inner join friendship f
    on f.friend_id = l.user_id
    and f.user_id =1)
    
select 
    distinct 
    friend_liked_page_id as "recommended_page"
    from liked_pages
    where friend_liked_page_id not in (
        select page_id as "user_liked_page" from Likes
        where user_id =1
    )
    order by 1;
'''

CASE 1 
Accepted
Runtime: 132 ms

Input
Friendship =
| user1_id | user2_id |
| -------- | -------- |
| 1        | 3        |
| 1        | 5        |
| 1        | 6        |
| 2        | 3        |
| 3        | 5        |
| 3        | 9        |

Likes =
| user_id | page_id |
| ------- | ------- |
| 6       | 13      |
| 8       | 10      |
| 9       | 14      |
  
Output
| recommended_page |
| ---------------- |
| 13               |
  
Expected
| recommended_page |
| ---------------- |
| 13               |


CASE 2 :

Input
Friendship =
| user1_id | user2_id |
| -------- | -------- |
| 1        | 2        |
| 1        | 3        |
| 1        | 4        |
| 2        | 3        |
| 2        | 4        |
| 2        | 5        |

View more
Likes =
| user_id | page_id |
| ------- | ------- |
| 1       | 88      |
| 2       | 23      |
| 3       | 24      |
| 4       | 56      |
| 5       | 11      |
| 6       | 33      |

View more
Output
| recommended_page |
| ---------------- |
| 23               |
| 24               |
| 33               |
| 56               |
| 77               |
Expected
| recommended_page |
| ---------------- |
| 23               |
| 24               |
| 56               |
| 33               |
| 77               |
'''
