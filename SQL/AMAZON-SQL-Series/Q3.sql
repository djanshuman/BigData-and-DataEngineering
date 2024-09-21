CREATE TABLE IF NOT EXISTS EMPLOYEE
(
    Employee_ID      VARCHAR(20) 
  , Employee_Name    VARCHAR(250)
  , Employee_City    VARCHAR(100)
  , Employee_DOB     DATE
  , Employee_Pin_Code VARCHAR(100)
  , Salary      INT
);

CREATE TABLE IF NOT EXISTS MAP_EMPLOYEE_HIERARCHY
(
    Employee_ID      VARCHAR(20) 
  , Manager_Employee_ID  VARCHAR(250)
);


INSERT INTO EMPLOYEE VALUES
        ('EMP1','Sarah','Phoenix',TO_DATE('1982-11-12', 'YYYY-MM-DD'), '85027',150000),
  ('EMP2','Mellisa','LA',TO_DATE('1980-04-24', 'YYYY-MM-DD'), '85027',200000),
  ('EMP3','John','Boston',TO_DATE('1986-02-18', 'YYYY-MM-DD'), '85027',180000),
  ('EMP4','Gary','NewMexico',TO_DATE('1975-02-12', 'YYYY-MM-DD'), '87121',170000),
  ('EMP5','Tony','SFO',TO_DATE('1960-01-08', 'YYYY-MM-DD'), '94016',300000),
  ('EMP6','Jason','Utah',TO_DATE('1972-05-25', 'YYYY-MM-DD'), '84118',110000),
     ('EMP7','Adam','Boston',TO_DATE('1970-02-18', 'YYYY-MM-DD'), '85027',250000),
  ('EMP8','Terry','NewMexico',TO_DATE('1980-02-12', 'YYYY-MM-DD'), '87121',300000),
  ('EMP9','Phil','SFO',TO_DATE('1978-01-08', 'YYYY-MM-DD'), '94016',270000),
  ('EMP10','David','Utah',TO_DATE('1980-05-25', 'YYYY-MM-DD'), '84118',280000);

INSERT INTO MAP_EMPLOYEE_HIERARCHY VALUES
        ('EMP1','EMP4'),
  ('EMP2','EMP5'),
  ('EMP3','EMP5'),
  ('EMP6','EMP4'),
        ('EMP4','EMP7'),
  ('EMP5','EMP8'),
  ('EMP8','EMP9'),
  ('EMP9','EMP10'),
  ('EMP10', 'EMP4'),
  ('EMP7','EMP4');

-- Question  -- Write a Query to display those employees who are making higher salary then their managers


with emp_salary as(
select 
	employee_id,
	salary 
	from employee
),
manager_salary as (
	select e1.employee_id,
	e2.manager_employee_id,
	e1.salary
	from emp_salary e1 inner join MAP_EMPLOYEE_HIERARCHY e2
	on e1.employee_id = e2.employee_id
	order by e1.employee_id)
	select distinct 
		manager_salary.employee_id
		from manager_salary inner join emp_salary
		on manager_salary.manager_employee_id =emp_salary.employee_id
		where manager_salary.salary > emp_salary.salary;


-- using self join option

with cte as (
select
	a.employee_id as emp_id,
	a.salary as emp_sal,
	b.manager_employee_id as mgr_emp_id,
	c.salary as mgr_sal
	from employee a
	inner join
	MAP_EMPLOYEE_HIERARCHY b
	on a.employee_id = b.employee_id
	inner join
	employee c
	on b.manager_employee_id = c.employee_id)
	select 
	emp_id
	from cte where emp_sal > mgr_sal;

