"""
https://datalemur.com/questions/compensation-outliers

Your team at Accenture is helping a Fortune 500 client revamp their compensation and benefits program. The first step in this analysis is to manually review employees who are potentially overpaid or underpaid.

An employee is considered to be potentially overpaid if they earn more than 2 times the average salary for people with the same title. Similarly, an employee might be underpaid if they earn less than half of the average for their title. We'll refer to employees who are both underpaid and overpaid as compensation outliers for the purposes of this problem.

Write a query that shows the following data for each compensation outlier: employee ID, salary, and whether they are potentially overpaid or potentially underpaid (refer to Example Output below).

employee_pay Table:
Column Name	Type
employee_id	integer
salary	integer
title	varchar
employee_pay Example Input:
employee_id	salary	title
101	80000	Data Analyst
102	90000	Data Analyst
103	100000	Data Analyst
104	30000	Data Analyst
105	120000	Data Scientist
106	100000	Data Scientist
107	80000	Data Scientist
108	310000	Data Scientist
Example Output:
employee_id	salary	status
104	30000	Underpaid
108	310000	Overpaid
Explanation
In this example, 2 employees qualify as compensation outliers. Employee 104 is a Data Analyst, and the average salary for this position is $75,000. Meanwhile, the salary of employee 104 is less than $37,500 (half of $75,000); therefore, they are underpaid.

The dataset you are querying against may have different input & output - this is just an example!
  
"""

with cte as (
SELECT
  e1.employee_id,
  e1.salary,
  e1.title,
  e2.avg_salary,
  CASE WHEN e1.salary >= (2 * e2.avg_salary) THEN 'Overpaid'
    WHEN e1.salary <= (e2.avg_salary/2) THEN 'Underpaid'
  END AS status
  from employee_pay e1 inner join
      (select 
        title,
        avg(salary) as "avg_salary"
        from employee_pay
  group by title) e2
  on e1.title= e2.title)
  SELECT
    employee_id,
    salary,
    status
    from cte where status is not null;

"""
Output

employee_id	salary	status
104	30000	Underpaid
108	310000	Overpaid
111	200000	Overpaid
112	25000	Underpaid

"""
