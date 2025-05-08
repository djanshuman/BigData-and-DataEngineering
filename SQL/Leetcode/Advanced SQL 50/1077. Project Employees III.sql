# https://leetcode.com/problems/project-employees-iii/description/?envType=study-plan-v2&envId=premium-sql-50

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

