-- SQL Windows Functions, RANK, GROUP BY

-- CREATE SCRIPT 
CREATE TABLE IF NOT EXISTS CUSTOMER
(
    Customer_ID      VARCHAR(20) 
  , Customer_Name    VARCHAR(250)
  , Customer_City    VARCHAR(100)
  , Customer_DOB     DATE
  , Customer_Pin_Code VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS CUSTOMER_TERRITORY
(
    Customer_ID      VARCHAR(20) 
  , TERRITORY_ID    VARCHAR(250)
  , Customer_City    VARCHAR(100)
  , Customer_Pin_Code VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS MAP_EMPLOYEE_TERRITORY
(
    Employee_ID      VARCHAR(20) 
  , Territory_ID    VARCHAR(250)
  , Product_SubCategory VARCHAR(250) 
);


--INSERT SCRIPTS

INSERT INTO CUSTOMER VALUES
        ('CUSTOMER1','Lindy','SanJose',TO_DATE('1988-02-24', 'YYYY-MM-DD'), '94088'),
        ('CUSTOMER2','Madison','Atlanta',TO_DATE('1984-05-28', 'YYYY-MM-DD'),30305),
  ('CUSTOMER3','Kiara','Austin',TO_DATE('1989-04-19', 'YYYY-MM-DD'),73301),
  ('CUSTOMER4','Beth','Ohio',TO_DATE('1992-08-18', 'YYYY-MM-DD'),43123),
  ('CUSTOMER5','Beth','SanAntonio',TO_DATE('1992-08-18', 'YYYY-MM-DD'),73504),
  ('CUSTOMER6','Beth','LasVegas',TO_DATE('1992-08-18', 'YYYY-MM-DD'),95403),
  ('CUSTOMER7','Lindy','LosAngeles',TO_DATE('1987-07-12', 'YYYY-MM-DD'),73504);

INSERT INTO CUSTOMER_TERRITORY VALUES
        ('CUSTOMER1','CA','SanJose', '94088'),
  ('CUSTOMER2','AZ','Atlanta','30305'),
  ('CUSTOMER3','UT','Austin','73301'),
  ('CUSTOMER4','OH','Ohio','43123'),
    ('CUSTOMER5','AZ','Phoenix', '85027'),
  ('CUSTOMER6','NV','LasVegas','95024');

INSERT INTO MAP_EMPLOYEE_TERRITORY VALUES
        ('EMP1','AZ', 'CLOTHES'),
  ('EMP2','CA', 'MENSWEAR'),
  ('EMP3','MA', 'WOMENWEAR'),
  ('EMP4','NM', 'ACCESSORIES'),
     ('EMP5','CA', 'JEWELERY'),
     ('EMP6','UT', 'KIDS');

	 
SELECT * FROM CUSTOMER;
SELECT * FROM CUSTOMER_TERRITORY;
SELECT * FROM MAP_EMPLOYEE_TERRITORY;

----QN1 - Write a Query to display those customer names and their occurrences when customer names appears more then once

SELECT CUSTOMER_NAME,
	  COUNT(CUSTOMER_NAME) AS "CUST_OCCUR"
	  FROM CUSTOMER
	  GROUP BY CUSTOMER_NAME
	  HAVING COUNT(CUSTOMER_NAME) > 1;


--QN2 - Write a Query to get the employees who are responsible for maximum number of customers
WITH CTE AS(
		SELECT 
		ET.EMPLOYEE_ID,
		COUNT(ET.TERRITORY_ID),
		DENSE_RANK() OVER (ORDER BY COUNT(ET.TERRITORY_ID) DESC) AS RNK
		FROM CUSTOMER_TERRITORY CT JOIN MAP_EMPLOYEE_TERRITORY ET
		ON CT.TERRITORY_ID = ET.TERRITORY_ID
		GROUP BY ET.EMPLOYEE_ID
		) SELECT EMPLOYEE_ID FROM CTE WHERE rnk=1;


-- QN3 - Write a Query to get those employees having no customers

select 
	EMPLOYEE_ID 
	from MAP_EMPLOYEE_TERRITORY 
	where TERRITORY_ID not in 
	( SELECT TERRITORY_ID FROM CUSTOMER_TERRITORY);



