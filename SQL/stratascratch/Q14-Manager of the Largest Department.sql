'''
https://platform.stratascratch.com/coding/2060-manager-of-the-largest-department?code_type=1

Manager of the Largest Department

Given a list of a company''s employees, find the name of the manager from the largest department. Manager is each employee that contains word "manager" under their position.  Output their first and last name.

az_employees

id	first_name	last_name	department_id	department_name	position
9	Christy	Mitchell	1001	Marketing	Senior specialist
13	Julie	Sanchez	1001	Marketing	Intern
14	John	Coleman	1001	Marketing	Senior specialist
15	Anthony	Valdez	1001	Marketing	Junior specialist
26	Allison	Johnson	1001	Marketing	Senior specialist
30	Stephen	Smith	1001	Marketing	Intern
38	Nicole	Lewis	1001	Marketing	Manager
41	John	George	1005	Sales	Junior specialist
44	Trevor	Carter	1001	Marketing	Senior specialist
53	Teresa	Cohen	1001	Marketing	Contractor
63	Richard	Sanford	1001	Marketing	Senior specialist
85	Meagan	Bullock	1001	Marketing	Senior specialist
89	Jason	Taylor	1001	Marketing	Junior specialist
97	Kelli	Moss	1001	Marketing	Contractor
3	Kelly	Rosario	1002	Human Resources	Senior specialist
5	Sherry	Golden	1002	Human Resources	Junior specialist
7	Diane	Gordon	1002	Human Resources	Contractor
11	Kevin	Townsend	1002	Human Resources	Senior specialist
18	Jeffrey	Harris	1002	Human Resources	Intern
21	Stephen	Berry	1002	Human Resources	Senior specialist
22	Brittany	Scott	1002	Human Resources	Junior specialist
33	Peter	Holt	1002	Human Resources	Junior specialist
39	Linda	Clark	1002	Human Resources	Intern
49	Amber	Harding	1002	Human Resources	Intern
50	Victoria	Wilson	1002	Human Resources	Interim manager
51	Theresa	Everett	1002	Human Resources	Junior specialist
56	Rachael	Williams	1005	Sales	Senior specialist
67	Tyler	Green	1002	Human Resources	Senior specialist
68	Antonio	Carpenter	1002	Human Resources	Senior specialist
81	Leah	Contreras	1002	Human Resources	Junior specialist
91	Jason	Burch	1002	Human Resources	Intern
92	Frances	Jackson	1002	Human Resources	Intern
95	Jillian	Potter	1002	Human Resources	Senior specialist
96	Sharon	Hunter	1002	Human Resources	Junior specialist
98	Jonathan	Jarvis	1002	Human Resources	Junior specialist
99	Kendra	Lynch	1002	Human Resources	Junior specialist
100	Amber	Miles	1002	Human Resources	Senior specialist
19	Michael	Ramsey	1003	Operations	Senior specialist
24	William	Flores	1003	Operations	Manager
27	Anthony	Ball	1003	Operations	Contractor
31	Kimberly	Brooks	1003	Operations	Senior specialist
34	Justin	Dunn	1003	Operations	Contractor
42	Traci	Williams	1003	Operations	Senior specialist
45	Kevin	Duncan	1003	Operations	Intern
46	Joshua	Ewing	1003	Operations	Senior specialist
47	Kimberly	Dean	1003	Operations	Contractor
61	Ryan	Brown	1003	Operations	Senior specialist
70	Karen	Fernandez	1003	Operations	Senior specialist
71	Kristine	Casey	1003	Operations	Intern
73	William	Preston	1003	Operations	Intern
74	Richard	Cole	1003	Operations	Junior specialist
77	Brittany	Harrison	1003	Operations	Junior specialist
83	Cameron	Webb	1003	Operations	Junior specialist
84	Jennifer	Mcneil	1003	Operations	Junior specialist
86	Jessica	Adams	1003	Operations	Senior specialist
4	Patricia	Powell	1004	Finance	Senior specialist
12	Joshua	Johnson	1004	Finance	Junior specialist
20	Cody	Gonzalez	1004	Finance	Junior specialist
23	Angela	Williams	1004	Finance	Senior specialist
35	John	Ball	1004	Finance	Junior specialist
40	Colleen	Carrillo	1004	Finance	Junior specialist
48	Robert	Lynch	1004	Finance	Manager
52	Kara	Smith	1004	Finance	Contractor
60	Charles	Pearson	1004	Finance	Intern
65	Deborah	Martin	1004	Finance	Intern
66	Dustin	Bush	1004	Finance	Intern
72	Christine	Frye	1004	Finance	Senior specialist
79	Katherine	Williams	1004	Finance	Intern
87	Gary	Martin	1004	Finance	Intern
88	Robert	Ortega	1004	Finance	Contractor
94	Vanessa	Lee	1004	Finance	Senior specialist
2	Justin	Simon	1005	Sales	Senior specialist
6	Natasha	Swanson	1005	Sales	Junior specialist
8	Mercedes	Rodriguez	1005	Sales	Junior specialist
16	Briana	Rivas	1005	Sales	Senior specialist
25	Pamela	Matthews	1005	Sales	Junior specialist
28	Alexis	Beck	1005	Sales	Senior specialist
36	Jesus	Ward	1005	Sales	Junior specialist
43	Joseph	Rogers	1005	Sales	Junior specialist
54	Wesley	Tucker	1005	Sales	Intern
55	Michael	Morris	1005	Sales	Senior specialist
57	Patricia	Harmon	1005	Sales	Senior specialist
58	Edward	Sharp	1005	Sales	Senior specialist
59	Kevin	Robinson	1005	Sales	Senior specialist
62	Dale	Hayes	1005	Sales	Senior specialist
69	Ernest	Peterson	1005	Sales	Intern
76	Melissa	Lee	1005	Sales	Junior specialist
78	Virginia	Mann	1005	Sales	Intern
80	Kimberly	Hawkins	1005	Sales	Intern
82	Melissa	Byrd	1005	Sales	Junior specialist
90	Richard	Garcia	1005	Sales	Manager
1	Todd	Wilson	1006	Engineering	Senior specialist
10	Sean	Crawford	1006	Engineering	Senior specialist
17	Jason	Burnett	1006	Engineering	Senior specialist
29	Jason	Olsen	1006	Engineering	Manager
32	Eric	Zimmerman	1006	Engineering	Junior specialist
37	Philip	Gillespie	1006	Engineering	Senior specialist
64	Danielle	Williams	1006	Engineering	Junior specialist
75	Julia	Ramos	1006	Engineering	Contractor
93	Frank	Newton	1006	Engineering	Contractor
'''


select first_name,
        last_name
from (
    select 
        *,
        RANK() over(order by dept_num desc) as rnk
    from
    (select *,
            count(*) over (partition by department_id) as dept_num
    from
    az_employees) a) b 
where POSITION ilike '%manager%'
    and rnk=1;


--other solution
select first_name,
        last_name 
from az_employees
where lower(position) like '%manager%'
and department_id in 
(
    select department_id 
    from
        (
            select 
                department_id,
                rank() over(order by count(*) desc) as rank
                from az_employees
                group by 1
        ) a 
        where rank=1);

'''
  
first_name	last_name
Victoria	Wilson
Richard	Garcia

'''
