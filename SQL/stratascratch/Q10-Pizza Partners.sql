'''
https://platform.stratascratch.com/coding/2016-pizza-partners?code_type=1  


Which partners have ‘pizza’ in their name and are located in Boston? And what is the average order amount? Output the partner name and the average order amount.

Tables: postmates_orders, postmates_markets, postmates_partners


postmates_orders:
  
id	customer_id	courier_id	seller_id	order_timestamp_utc	amount	city_id
1	102	224	79	2019-03-11 23:27:00	155.73	47
2	104	224	75	2019-04-11 04:24:00	216.6	44
3	100	239	79	2019-03-11 21:17:00	168.69	47
4	101	205	79	2019-03-11 02:34:00	210.84	43
5	103	218	71	2019-04-11 00:15:00	212.6	47
6	102	201	77	2019-03-11 18:22:00	220.83	47
7	103	205	79	2019-04-11 11:15:00	94.86	49
8	101	246	77	2019-03-11 04:12:00	86.15	49
9	101	218	79	2019-03-11 08:59:00	75.52	43
10	103	211	77	2019-03-11 00:22:00	15.85	49
11	102	223	79	2019-03-11 10:44:00	59.69	49
12	104	211	77	2019-03-11 01:37:00	153.61	44
13	100	204	71	2019-03-11 07:00:00	190.29	43
14	100	231	79	2019-03-11 03:12:00	115.45	49
15	104	246	75	2019-04-11 08:52:00	225.91	47
16	102	231	77	2019-03-11 08:26:00	158.68	43
17	102	246	79	2019-04-11 01:15:00	62.72	49
18	100	205	77	2019-03-11 03:10:00	125.65	49
19	104	204	77	2019-04-11 08:14:00	129.75	44
20	101	231	75	2019-04-11 10:49:00	105.07	43

postmates_markets:

id	name	timezone
43	Boston	EST
44	Seattle	PST
47	Denver	MST
49	Chicago	CST


postmates_partners:

id	name	category
71	Papa John's	Pizza
75	Domino's Pizza	Pizza
77	Pizza Hut	Pizza
79	Papa Murphy's	Pizza

  
'''


with cte as (
    select 
    pp.name as name,
    avg(po.amount) as avg
    from postmates_orders po
        inner join
        postmates_markets pm on po.city_id = pm.id
        inner join postmates_partners pp
        on pp.id = po.seller_id
        where pm.name = 'Boston'
        group by 1)
select * from 
cte where lower(name) like '%pizza%';


'''

Your Solution:
name	avg
Domino's Pizza	105.07
Pizza Hut	158.68
  
'''
