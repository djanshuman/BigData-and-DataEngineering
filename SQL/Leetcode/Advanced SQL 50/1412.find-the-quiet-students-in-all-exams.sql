'''
https://leetcode.com/problems/find-the-quiet-students-in-all-exams/description/?envType=study-plan-v2&envId=premium-sql-50

Table: Student

+---------------------+---------+
| Column Name         | Type    |
+---------------------+---------+
| student_id          | int     |
| student_name        | varchar |
+---------------------+---------+
student_id is the primary key (column with unique values) for this table.
student_name is the name of the student.
 

Table: Exam

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| exam_id       | int     |
| student_id    | int     |
| score         | int     |
+---------------+---------+
(exam_id, student_id) is the primary key (combination of columns with unique values) for this table.
Each row of this table indicates that the student with student_id had a score points in the exam with id exam_id.
 

A quiet student is the one who took at least one exam and did not score the highest or the lowest score.

Write a solution to report the students (student_id, student_name) being quiet in all exams. Do not return the student who has never taken any exam.

Return the result table ordered by student_id.

The result format is in the following example.

 

Example 1:

Input: 
Student table:
+-------------+---------------+
| student_id  | student_name  |
+-------------+---------------+
| 1           | Daniel        |
| 2           | Jade          |
| 3           | Stella        |
| 4           | Jonathan      |
| 5           | Will          |
+-------------+---------------+
Exam table:
+------------+--------------+-----------+
| exam_id    | student_id   | score     |
+------------+--------------+-----------+
| 10         |     1        |    70     |
| 10         |     2        |    80     |
| 10         |     3        |    90     |
| 20         |     1        |    80     |
| 30         |     1        |    70     |
| 30         |     3        |    80     |
| 30         |     4        |    90     |
| 40         |     1        |    60     |
| 40         |     2        |    70     |
| 40         |     4        |    80     |
+------------+--------------+-----------+
Output: 
+-------------+---------------+
| student_id  | student_name  |
+-------------+---------------+
| 2           | Jade          |
+-------------+---------------+
Explanation: 
For exam 1: Student 1 and 3 hold the lowest and high scores respectively.
For exam 2: Student 1 hold both highest and lowest score.
For exam 3 and 4: Studnet 1 and 4 hold the lowest and high scores respectively.
Student 2 and 5 have never got the highest or lowest in any of the exams.
Since student 5 is not taking any exam, he is excluded from the result.
So, we only return the information of Student 2.
'''

-- Write your PostgreSQL query statement below
with grouping as (
  select 
    exam_id, max(e1.score) as max ,min(e1.score) as min
        from Exam e1 
        group by exam_id),
mapping as (
 select
    s.student_name,
    e.student_id,
    (case when e.score in (c.max,c.min) then false 
    else true end) as filter
    from Student s inner join Exam e 
    on s.student_id = e.student_id
    inner join grouping c on 
    e.exam_id = c.exam_id)
select  
    distinct
    student_id,
    student_name
    from mapping
    where student_id not in (
        select student_id from mapping
        where filter = 'false')
    and filter = 'true'
    order by 1;

'''
Accepted
Runtime: 213 ms
Case 1
Input
Student =
| student_id | student_name |
| ---------- | ------------ |
| 1          | Daniel       |
| 2          | Jade         |
| 3          | Stella       |
| 4          | Jonathan     |
| 5          | Will         |
Exam =
| exam_id | student_id | score |
| ------- | ---------- | ----- |
| 10      | 1          | 70    |
| 10      | 2          | 80    |
| 10      | 3          | 90    |
| 20      | 1          | 80    |
| 30      | 1          | 70    |
| 30      | 3          | 80    |

View more
Output
| student_id | student_name |
| ---------- | ------------ |
| 2          | Jade         |
Expected
| student_id | student_name |
| ---------- | ------------ |
| 2          | Jade         |
  
'''

'''
Notes
1.In CTE , we find the min and max score per exam ID
2.In mapping , we join and apply a case statement to check , if any student score is equal to max and min for that exam and store result in filter column.
3.In final select, we first fetch student_id where filter='false' ( the id's who were part of max or min score) and then filter and select those id's who are having filter true means not part of max or min score.
Reason for doing this , ID 3 was max score in exam 10 but in other exam id , it was not part of max or min, however we cannot consider it as it was max or min at least once.

'''
