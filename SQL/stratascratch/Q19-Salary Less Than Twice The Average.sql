'''
https://platform.stratascratch.com/coding/2110-salary-less-than-twice-the-average?code_type=1

Write a query to get the list of managers whose salary is less than twice the average salary of employees reporting to them. For these managers, output their ID, salary and the average salary of employees reporting to them.

map_employee_hierarchy

empl_id	manager_empl_id
E849	
E850	E849
E851	E849
E852	E850
E853	E850
E854	E851
E855	E851
E856	E851
E857	E854


dim_employee

empl_id	empl_name	empl_city	empl_dob	empl_pin	salary
E849	Steven M. Jones	Hicksville	1988-03-29	1490	80000
E850	Marilynn M. Walters	New York	1978-12-26	9624	30000
E851	Kyle M. Massey	Lake Katrine	1977-09-22	1563	40000
E852	Cody A. Mosby	Anaheim	1965-03-18	4883	22000
E853	David J. Mintz	Houston	1977-01-04	8001	18000
E854	Patricia J. Kyser	Atlanta	1986-02-20	1750	18000
E855	Mark M. Daniels	Atlanta	1979-07-13	2424	20000
E856	Gene M. Vanscoy	Chicago	1977-03-11	1107	16000
E857	Mitchell A. Grimm	Houston	1979-11-23	8597	16000
  
'''


-- solution 1 --
select
    m.manager_empl_id,
    me.salary,
    avg(e.salary) avg_emp_salary
from map_employee_hierarchy m 
inner join dim_employee me on m.manager_empl_id = me.empl_id
inner join dim_employee e  on m.empl_id = e.empl_id
group by 1,2
having me.salary < 2 * avg(e.salary);


-- solution  2 --
SELECT h.manager_empl_id,
       managers.salary AS manager_salary,
       AVG(employees.salary) AS avg_employee_salary
FROM map_employee_hierarchy h
JOIN dim_employee managers ON h.manager_empl_id = managers.empl_id
JOIN dim_employee employees ON h.empl_id = employees.empl_id
GROUP BY 1,2
HAVING managers.salary < 2 * AVG(employees.salary);


'''
manager_empl_id	salary	avg_emp_salary
E850	30000	20000
E854	18000	16000
'''
