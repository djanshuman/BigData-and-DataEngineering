'''
https://platform.stratascratch.com/coding/10353-workers-with-the-highest-salaries?code_type=1

Find the job titles of the employees with the highest salary. If multiple employees have the same highest salary, include the job titles for all such employees.

'''

with cte as 
(select 
    w.worker_id,
    w.salary,
    t.worker_title as best_paid_title,
    dense_rank() over(order by w.salary desc) as "rnk"
    from worker w inner join title t on w.worker_id = t.worker_ref_id)
    select 
        best_paid_title
        from cte where rnk=1;
    
