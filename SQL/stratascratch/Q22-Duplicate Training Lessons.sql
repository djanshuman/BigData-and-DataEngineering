'''
https://platform.stratascratch.com/coding/2130-duplicate-training-lessons?code_type=1

Display a list of users who took the same training lessons more than once on the same day. Output their usernames, training IDs, dates and the number of times they took the same lesson.

Tables: users_training, training_details

users_training

u_id	u_name
1	Lee4an
2	pego_tinn
3	Jenna1021
4	whoisthatgirl1
5	Fakehashish
6	dontlookatme20
7	TheeeHman
8	doglover23
9	theblacktaco
10	idkwhattodoy
11	olivetree0008
12	jtothew
13	vicsloan
14	katajin31309
15	catalina0816

training_details
  
u_t_id	u_id	training_id	training_date
1	1	1	2022-08-01
2	7	4	2022-08-01
3	4	3	2022-08-01
4	3	2	2022-08-01
5	12	5	2022-08-01
6	9	3	2022-08-01
7	1	1	2022-08-01
8	5	2	2022-08-02
9	15	4	2022-08-02
10	5	2	2022-08-02
11	11	1	2022-08-04
'''
with cte as (
select * from (
    select 
        training_date,
        u_id,
        training_id,
        count(*) as n_attended
        from training_details
        group by 1,2,3
        having count(*) > 1)a
    ) 
select
    ut.u_name,
    cte.training_id,
    cte.training_date,
    cte.n_attended
from cte inner join users_training ut
on ut.u_id = cte.u_id;


'''
u_name	training_id	training_date	n_attended
Lee4an	1	2022-08-01	2
pego_tinn	2	2022-08-08	2
pego_tinn	1	2022-08-12	2
Jenna1021	3	2022-08-05	2
whoisthatgirl1	1	2022-08-20	2
whoisthatgirl1	4	2022-08-09	2
Fakehashish	2	2022-08-02	2
Fakehashish	1	2022-08-12	2
dontlookatme20	5	2022-08-12	3
dontlookatme20	3	2022-08-17	3
TheeeHman	3	2022-08-08	2
doglover23	5	2022-08-10	2
idkwhattodoy	2	2022-08-15	2
idkwhattodoy	3	2022-08-13	2
olivetree0008	1	2022-08-04	2
vicsloan	3	2022-08-20	3
catalina0816	3	2022-08-09	2
'''
  
