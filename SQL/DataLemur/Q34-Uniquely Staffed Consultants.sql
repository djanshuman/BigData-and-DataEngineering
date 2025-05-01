"""
https://datalemur.com/questions/uniquely-staffed-consultants

As a Data Analyst on the People Operations team at Accenture, you are tasked with understanding how many consultants are staffed to each client, and specifically how many consultants are exclusively staffed to a single client.

Write a query that displays the client name along with the total number of consultants attached to each client, and the number of consultants who are exclusively staffed to each client (consultants working exclusively for that client). Ensure the results are ordered alphabetically by client name.

As of July 5th, the datasets have been expanded to cover edge cases and the solutions have been revised accordingly.

employees Table:
Column Name	Type
employee_id	integer
engagement_id	integer
employees Example Input:
employee_id	engagement_id
1001	1
1001	2
1002	1
1002	8
1003	3
1003	4
1004	3
1004	4
1005	5
1005	6
1005	7
  
consulting_engagements Table:
Column Name	Type
engagement_id	integer
project_name	string
client_name	string
  
consulting_engagements Example Input:
engagement_id	project_name	client_name
1	SAP Logistics Modernization	Department of Defense
2	Oracle Cloud Migration	Department of Education
3	Trust & Safety Operations	Google
4	SAP IoT Cloud Integration	Google
  
Example Output:
client_name	total_consultants	single_client_consultants
Department of Defense	2	1
Department of Education	1	0
Google	2	2
  
Explanation:
Department of Defense: Total consultants are 2 (1001 and 1002), and 1 consultant (1002) is exclusively staffed.
Department of Education: Total consultant is 1 (1001), but not exclusively staffed as 1002 is also staffed to engagement ID 8.
Google: Total consultants are 2 (1003 and 1004), and both consultants are exclusively staffed.
The dataset you are querying against may have different input & output - this is just an example!
  
"""



-- uniquely staffed consultant
with single_client_consultants as (
SELECT
  employee_id
  from employees inner join consulting_engagements
  on employees.engagement_id = consulting_engagements.engagement_id
 group by employee_id
  having count(distinct client_name) = 1)
  
  
--total_consultants
SELECT
  eg.client_name,
  count(distinct e.employee_id) as "total_consultants",
  count(single.employee_id) as "single_client_consultants"
  from employees e inner join consulting_engagements eg
  on e.engagement_id = eg.engagement_id
  left outer join single_client_consultants as single
  on e.employee_id = single.employee_id
  group by 1
  order by 1;

"""
Output

client_name	          total_consultants	single_client_consultants
Department of Defense	  2	                0
Department of Education	1                	0
Google	                4	                4
Hubspot	                3	                4
Microsoft	              3	                3
Tesla	                  3	                0

"""
