'''
https://platform.stratascratch.com/coding/10152-workers-with-the-highest-and-lowest-salaries?code_type=1
You have been asked to find the employees with the highest and lowest salary across whole dataset.


Your output should include the employees ID, salary, and employees department, as well as a column salary_type that categorizes the output by:



'Highest Salary' represents the highest salary

'Lowest Salary' represents the lowest salary

'''
with cte as (
select 
    worker_id,
    salary,
    department,
    max(salary) over() as "max_salary",
    min(salary) over () as "min_salary",
    case 
        when salary = max(salary) over() then 'Highest Salary'
        when salary = min(salary) over() then 'Lowest Salary'
        else null end as "salary_type"
    from worker)
    
select 
    worker_id,
    salary,
    department,
    salary_type
    from cte where salary_type is not null;

'''
Output
View the output in a separate browser tab
Execution time: 0.04147 seconds

Your Solution:
worker_id	salary	department	salary_type
4	500000	Admin	Highest Salary
5	500000	Admin	Highest Salary
10	65000	HR	Lowest Salary

'''
