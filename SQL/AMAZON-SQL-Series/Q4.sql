

CREATE TABLE IF NOT EXISTS CUST_DIM
(
    CUST_ID  VARCHAR(20) 
  , FULL_NAME    VARCHAR(250)
  , BIRTH_DATE   DATE
  , ITEM_PURCHASE_DATE   DATE
  , ITEM_SPEND_AMT INT
  , CITY  VARCHAR(100)
  , STATE VARCHAR(100)
  , COUNTRY VARCHAR(100)
);

-- INSERT SCRIPT

 INSERT INTO CUST_DIM VALUES
 ('1','Lisa Johnson',TO_DATE('1980-02-24', 'YYYY-MM-DD'), TO_DATE('2022-02-05', 'YYYY-MM-DD'), 1200, 'LV', 'CA','US'),
 ('2','Madison Pierson',TO_DATE('1984-05-28', 'YYYY-MM-DD'), TO_DATE('2022-04-10', 'YYYY-MM-DD'),1400,'Atlanta','GA','US'),
 ('3','Kiara Sharma',TO_DATE('1978-04-19', 'YYYY-MM-DD'),TO_DATE('2022-05-08', 'YYYY-MM-DD'),2000,'Austin', 'TX','US'),
 ('4','Beth Hood',TO_DATE('1960-08-18', 'YYYY-MM-DD'),TO_DATE('2022-03-10', 'YYYY-MM-DD'),800,'Cleveland','OH','US'),
 ('5','Susan Holmes',TO_DATE('1992-08-18', 'YYYY-MM-DD'),TO_DATE('2022-01-10', 'YYYY-MM-DD'),700,'SanAntonio','TX', 'US'),
 ('6','Andre Lyn',TO_DATE('1990-08-18', 'YYYY-MM-DD'),TO_DATE('2021-05-10', 'YYYY-MM-DD'),800,'TorontoCity','Toronto', 'CANADA'),
 ('7','Becky Stone',TO_DATE('1968-07-12', 'YYYY-MM-DD'),TO_DATE('2022-04-10', 'YYYY-MM-DD'),1200,'TorontoCity','Toronto', 'CANADA'),
 ('8','Rocky Johnson',TO_DATE('1988-02-12', 'YYYY-MM-DD'),TO_DATE('2022-01-10', 'YYYY-MM-DD'),3000,'LosAngeles','CA', 'US'),
 ('9','Kim Advani',TO_DATE('1980-03-24', 'YYYY-MM-DD'),TO_DATE('2022-04-10', 'YYYY-MM-DD'),4000,'LosAngeles','CA', 'US'),
 ('10','Sunny Walker',TO_DATE('1979-03-24', 'YYYY-MM-DD'),TO_DATE('2022-04-10', 'YYYY-MM-DD'),1400,'Paris','Paris', 'France');
 
-- How many customers older than 40 years old, by state, have spent at least 1000 USD and residing in US?


with cte as (
select 
	CUST_ID,
	ITEM_SPEND_AMT,
	STATE
	from CUST_DIM
	where extract(year from current_date) - extract( year from BIRTH_DATE) > 40
	and ITEM_SPEND_AMT >= 1000)
	select 
	STATE,
	count(distinct CUST_ID)
	from cte
	group by STATE;
