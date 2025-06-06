# https://leetcode.com/problems/project-employees-iii/description/?envType=study-plan-v2&envId=premium-sql-50
'''
Table: Project

+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| project_id  | int     |
| employee_id | int     |
+-------------+---------+
(project_id, employee_id) is the primary key (combination of columns with unique values) of this table.
employee_id is a foreign key (reference column) to Employee table.
Each row of this table indicates that the employee with employee_id is working on the project with project_id.
 

Table: Employee

+------------------+---------+
| Column Name      | Type    |
+------------------+---------+
| employee_id      | int     |
| name             | varchar |
| experience_years | int     |
+------------------+---------+
employee_id is the primary key (column with unique values) of this table.
Each row of this table contains information about one employee.
 

Write a solution to report the most experienced employees in each project. In case of a tie, report all employees with the maximum number of experience years.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Project table:
+-------------+-------------+
| project_id  | employee_id |
+-------------+-------------+
| 1           | 1           |
| 1           | 2           |
| 1           | 3           |
| 2           | 1           |
| 2           | 4           |
+-------------+-------------+
Employee table:
+-------------+--------+------------------+
| employee_id | name   | experience_years |
+-------------+--------+------------------+
| 1           | Khaled | 3                |
| 2           | Ali    | 2                |
| 3           | John   | 3                |
| 4           | Doe    | 2                |
+-------------+--------+------------------+
Output: 
+-------------+---------------+
| project_id  | employee_id   |
+-------------+---------------+
| 1           | 1             |
| 1           | 3             |
| 2           | 1             |
+-------------+---------------+
Explanation: Both employees with id 1 and 3 have the most experience among the employees of the first project. For the second project, the employee with id 1 has the most experience.
    '''
    
-- Write your PostgreSQL query statement below
WITH CTE AS (
SELECT
    E.employee_id,
    E.experience_years,
    P.project_id,
    DENSE_RANK() OVER(PARTITION BY P.project_id ORDER BY E.experience_years desc) AS "rnk"
    FROM Employee E right outer join Project P
    ON E.employee_id = P.employee_id)
    SELECT 
        project_id,
        employee_id
        FROM CTE WHERE rnk =1
        order by 1,2;
'''
Accepted
Runtime: 132 ms
Case 1
Input
Project =
| project_id | employee_id |
| ---------- | ----------- |
| 1          | 1           |
| 1          | 2           |
| 1          | 3           |
| 2          | 1           |
| 2          | 4           |
Employee =
| employee_id | name   | experience_years |
| ----------- | ------ | ---------------- |
| 1           | Khaled | 3                |
| 2           | Ali    | 2                |
| 3           | John   | 3                |
| 4           | Doe    | 2                |
Output
| project_id | employee_id |
| ---------- | ----------- |
| 1          | 1           |
| 1          | 3           |
| 2          | 1           |
Expected
| project_id | employee_id |
| ---------- | ----------- |
| 1          | 1           |
| 1          | 3           |
| 2          | 1           |
'''
