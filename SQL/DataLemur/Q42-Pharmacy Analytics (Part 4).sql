"""
https://datalemur.com/questions/top-drugs-sold

CVS Health is trying to better understand its pharmacy sales, and how well different drugs are selling.

Write a query to find the top 2 drugs sold, in terms of units sold, for each manufacturer. List your results in alphabetical order by manufacturer.

pharmacy_sales Table:
Column Name	Type
product_id	integer
units_sold	integer
total_sales	decimal
cogs	decimal
manufacturer	varchar
drug	varchar
pharmacy_sales Example Input:
product_id	units_sold	total_sales	cogs	manufacturer	drug
94	132362	2041758.41	1373721.70	Biogen	UP and UP
9	37410	293452.54	208876.01	Eli Lilly	Zyprexa
50	90484	2521023.73	2742445.9	Eli Lilly	Dermasorb
61	77023	500101.61	419174.97	Biogen	Varicose Relief
136	144814	1084258.00	1006447.73	Biogen	Burkhart
109	118696	1433109.50	263857.96	Eli Lilly	Tizanidine Hydrochloride
Example Output:
manufacturer	top_drugs
Biogen	Burkhart
Biogen	UP and UP
Eli Lilly	Tizanidine Hydrochloride
Eli Lilly	TA Complete Kit
Explanation
Biogen sold 144,814 units of Burkhart drug (ranked 1) followed by the second highest with 132,362 units of UP and UP drug (ranked 2).

Eli Lilly sold 118,696 units of Tizanidine Hydrochloride drug (ranked 1) followed by the second highest with 90,484 units of TA Complete Kit drug (ranked 2).

The dataset you are querying against may have different input & output - this is just an example!
"""

WITH CTE AS (
SELECT
  manufacturer,
  units_sold,
  drug as top_drugs,
  row_number() OVER(PARTITION BY manufacturer ORDER BY units_sold DESC) AS "top_rank"
  FROM pharmacy_sales)
  select 
    manufacturer,
    top_drugs
    from CTE where top_rank <=2;

"""
Output

manufacturer	top_drugs
  
AbbVie	Lamivudine and Zidovudine
AbbVie	Clarithromycin
AstraZeneca	BANANA BOAT SUNSCREEN
AstraZeneca	MiraLAX
Bayer	Diphenhydramine HCL
Bayer	Citalopram Hydrobromide
Biogen	DIPHENHYDRAMINE HYDROCHLORIDE
Biogen	Nefazodone Hydrochloride
Eli Lilly	Cialis
Eli Lilly	Metoclopramide
GlaxoSmithKline	eszopiclone
GlaxoSmithKline	Isoniazid
Johnson & Johnson	JUNIPERUS ASHEI POLLEN
Johnson & Johnson	Hand wash
Merck	Keytruda
Merck	DHEA
Novartis	Imatinib
Novartis	ANASTROZOLE
Pfizer	Ropinirole Hydrochloride
Pfizer	Stay Awake
Roche	Lucentis
Roche	Dorzolamide HCl
Sanofi	Dupixent
Sanofi	Lovenox
"""
