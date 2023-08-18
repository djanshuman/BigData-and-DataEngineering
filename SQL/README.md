1. Select top N salaries from EMP table

-- second highest 

select * from
(select e.*,
    dense_rank() over (order by sal desc) rnk from emp e )
where rnk =2;
--7701	John	WORKER	5000	2

-- Second highest 

select * from emp where sal = (
select max(sal) from emp where sal not in (select max(sal) from emp ));
--7701	John	WORKER	5000


-- nth highest, just replace  n with a value = or < 

select * from
(select e.*,
    dense_rank() over (order by sal desc) rnk from emp e )
where rnk =n;


2. top n salary or nth highest salary each dept wise. 

select * from (
select e.* , dense_rank() over (partition by JOB order by sal desc) rnk from emp e) where rnk =1;

select * from (
select e.* , dense_rank() over (partition by JOB order by sal desc) rnk from emp e) where rnk <3;


3. Query to select/delete duplicate rows from a table

select e.*,rowid from emp1 e where e.rowid not in (
select min(rowid) from emp1 e group by empno);



7699	Alex	MANAGER	2000	AK+5c0ADCAAAAE0AAA
7700	Adam	STAFF	3000	AK+5c0ADCAAAAE0AAB
7700	Adam	STAFF	3000	AK+5c0ADCAAAAE0AAC
7702	Mark	STAFF	4000	AK+5c0ADCAAAAE0AAD
7702	Mark	STAFF	4000	AK+5c0ADCAAAAE0AAE
7704	Rauf	STAFF	3000	AK+5c0ADCAAAAE0AAF
7705	Rudy	WORKER	4000	AK+5c0ADCAAAAE0AAG
7705	Rudy	WORKER	4000	AK+5c0ADCAAAAE0AAH
7707	HOF		MANAGER	3000	AK+5c0ADCAAAAE0AAI
7707	HOF		MANAGER	3000	AK+5c0ADCAAAAE0AAJ

-- Inner query (select min(rowid) from emp1 e group by empno)
EMPNO	MIN(ROWID)
7705	AK+5c0ADCAAAAE0AAG
7699	AK+5c0ADCAAAAE0AAA
7700	AK+5c0ADCAAAAE0AAB
7702	AK+5c0ADCAAAAE0AAD
7707	AK+5c0ADCAAAAE0AAI
7704	AK+5c0ADCAAAAE0AAF

--Output 
EMPNO	ENAME	JOB	SAL	ROWID
7702	Mark	STAFF	4000	AK+5c0ADCAAAAE0AAE
7707	HOF		MANAGER	3000	AK+5c0ADCAAAAE0AAJ
7700	Adam	STAFF	3000	AK+5c0ADCAAAAE0AAC
7705	Rudy	WORKER	4000	AK+5c0ADCAAAAE0AAH

delete from emp1 e where e.rowid not in (
select min(rowid) from emp1 e group by empno);
4 row(s) deleted.


4. Write a query to select only those employee information who are earning the same salary?


select e1.* from emp1 e1 join emp1 e2 on e1.sal=e2.sal and e1.ename <> e2.ename order by e1.sal, e1.job;

select * from emp where sal in (
select sal from emp1 group by sal having count(sal) > 1) order by sal,job; 


5. Write a query to display even/odd number rows from a table.

select * from 
(select e.* , rownum rn from emp1 e order by e.empno) where mod(rn,2) <> 0;


select * from 
(select e.* , rownum rn from emp1 e order by e.empno) where mod(rn,2) = 0;


5. Write a query to find 4th highest salary (without analytical function)


select * from emp1 e1 where (3-1) = (select count(distinct(e2.sal)) from emp1 e2 where e2.sal > e1.sal)

select * from emp1 e1 where (N-1) = (select count(distinct(e2.sal)) from emp1 e2 where e2.sal > e1.sal)

6. Write a query to display the employee information, who have more than 2 employees under a manager

select * from ( SELECT e.*, count(mgr) over (partition by mgr) as cnt from emp e ) where cnt >= 2

7. Write a query to find the maximum salary from the EMP table without using functions.

select * from emp1 e1 where e1.sal not in (select e2.sal from emp1 e2 inner join emp1 e1 on e2.sal < e1.sal);

select * from emp where sal not in ( select A.sal from emp A, emp B where A.sal < B.sal )



8. Write a query to select the last N records from a table…

-- Select last 3 rows from table

select * from emp1   -- this will give 10 - top 7 rows = last 3 rows 
    minus 
select * from emp1 where rownum <= ( --- this will give top 7 rows ) 
select count(*) - 3 from emp1); --total rows in tbl = 10 , this query will give 7 output 
    

9. Write a query to find the employees who are working in the company for the past 5 years.

select * from emp where hiredate < add_months(sysdate,-60);


10. Write a query to find the number of rows in a table without using COUNT function.


select max(r) from (
(select e.*, rownum r from emp1 e));

select max(r) from (
select row_number() over (order by empno desc) as r from emp1 ;
)

11. Write a query to find the LAST inserted record in a table.

If you want the last record inserted, you need to have a timestamp or sequence number assigned to each record as they are inserted and then you can use the below query. 

select * from t where TIMESTAMP_COLUMN = (select max(timestamp_column) from T) and rownum = 1;
