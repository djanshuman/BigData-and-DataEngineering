'''
https://leetcode.com/problems/all-people-report-to-the-given-manager/description/?envType=study-plan-v2&envId=premium-sql-50

SQL Schema
Pandas Schema
Table: Employees

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| employee_id   | int     |
| employee_name | varchar |
| manager_id    | int     |
+---------------+---------+
employee_id is the column of unique values for this table.
Each row of this table indicates that the employee with ID employee_id and name employee_name reports his work to his/her direct manager with manager_id
The head of the company is the employee with employee_id = 1.
 

Write a solution to find employee_id of all employees that directly or indirectly report their work to the head of the company.

The indirect relation between managers will not exceed three managers as the company is small.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Employees table:
+-------------+---------------+------------+
| employee_id | employee_name | manager_id |
+-------------+---------------+------------+
| 1           | Boss          | 1          |
| 3           | Alice         | 3          |
| 2           | Bob           | 1          |
| 4           | Daniel        | 2          |
| 7           | Luis          | 4          |
| 8           | Jhon          | 3          |
| 9           | Angela        | 8          |
| 77          | Robert        | 1          |
+-------------+---------------+------------+
Output: 
+-------------+
| employee_id |
+-------------+
| 2           |
| 77          |
| 4           |
| 7           |
+-------------+
Explanation: 
The head of the company is the employee with employee_id 1.
The employees with employee_id 2 and 77 report their work directly to the head of the company.
The employee with employee_id 4 reports their work indirectly to the head of the company 4 --> 2 --> 1. 
The employee with employee_id 7 reports their work indirectly to the head of the company 7 --> 4 --> 2 --> 1.
The employees with employee_id 3, 8, and 9 do not report their work to the head of the company directly or indirectly. 
'''
SELECT
    E1.employee_id
    FROM Employees E1 INNER JOIN Employees E2
    ON E1.manager_id = E2.employee_id
    INNER JOIN Employees E3
    ON E2.manager_id = E3.employee_id
    AND E3.manager_id = 1
    AND E1.employee_id != 1;
'''
  Accepted
Runtime: 148 ms
Case 1
Input
Employees =
| employee_id | employee_name | manager_id |
| ----------- | ------------- | ---------- |
| 1           | Boss          | 1          |
| 3           | Alice         | 3          |
| 2           | Bob           | 1          |
| 4           | Daniel        | 2          |
| 7           | Luis          | 4          |
| 8           | John          | 3          |

View more
Output
| employee_id |
| ----------- |
| 2           |
| 4           |
| 7           |
| 77          |
Expected
| employee_id |
| ----------- |
| 2           |
| 4           |
| 7           |
| 77          |
  
'''
'''
--Notes --
--- Tree Level ---


SELECT 
    e1.employee_id as "employee_id",
    e1.employee_name as "employee_name",
    e1.manager_id as "employee_manager_id",
    
    e2.employee_id as "employee_manager_id",
    e2.employee_name as "employee_manager_name",
    e2.manager_id as "spm_manager_id",
    
    e3.employee_id as "spm_manager_id",
    e3.employee_name as "spm_manager_name",
    e3.manager_id as "director_manager_id "
    
FROM Employees e1
JOIN Employees e2
ON e1.manager_id = e2.employee_id
inner join Employees e3
on e2.manager_id = e3.employee_id
where e1.employee_id != 1


| employee_id | employee_name | employee_manager_id | employee_manager_id | employee_manager_name | spm_manager_id | spm_manager_id | spm_manager_name | director_manager_id  |
| ----------- | ------------- | ------------------- | ------------------- | --------------------- | -------------- | -------------- | ---------------- | -------------------- |
| 77          | Robert        | 1                   | 1                   | Boss                  | 1              | 1              | Boss             | 1                    |
| 2           | Bob           | 1                   | 1                   | Boss                  | 1              | 1              | Boss             | 1                    |
| 4           | Daniel        | 2                   | 2                   | Bob                   | 1              | 1              | Boss             | 1                    |
| 7           | Luis          | 4                   | 4                   | Daniel                | 2              | 2              | Bob              | 1                    |
| 8           | John          | 3                   | 3                   | Alice                 | 3              | 3              | Alice            | 3                    |
| 9           | Angela        | 8                   | 8                   | John                  | 3              | 3              | Alice            | 3                    |
| 3           | Alice         | 3                   | 3                   | Alice                 | 3              | 3              | Alice            | 3                    |





Employees table:
+-------------+---------------+------------+
| employee_id | employee_name | manager_id |
+-------------+---------------+------------+
| 1           | Boss          | 1          | -- Level 0 (e3), we find director manager_id details //(assume Manager_id is not 1 but some other value , may be 10 ), So we need to reach here, e3.manager_id is the filter
| 3           | Alice         | 3          |
| 2           | Bob           | 1          | -- Level 1 (e2) ,we find spm manager_id details 
| 4           | Daniel        | 2          | -- Level 2 (e1) ,we find PM manager_id details
| 7           | Luis          | 4          |
| 8           | Jhon          | 3          |
| 9           | Angela        | 8          |
| 77          | Robert        | 1          |
+-------------+---------------+------------+
'''
