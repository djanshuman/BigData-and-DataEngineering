'''
Calculate Average Score

https://platform.stratascratch.com/coding/10540-calculate-average-score?code_type=1


project_id	team_member_id	score	date
101	1	5	2024-07-25
101	2	8	2024-09-22
101	2	3	2024-09-24
101	2	5	2024-10-14
101	6	6	2024-10-14
'''

select project_id,avg(score) 
from project_data 
group by project_id 
having count(team_member_id) > 1;


'''

Output
View the output in a separate browser tab
Execution time: 0.00469 seconds

project_id	avg
101	      5.33
103	      56.82
104	      5.46
105	    14.25
102	      3
'''
