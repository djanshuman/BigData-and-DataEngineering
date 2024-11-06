'''
https://platform.stratascratch.com/coding/2015-city-with-the-highest-and-lowest-income-variance?code_type=1


City With The Highest and Lowest Income Variance


Interview Question Date: January 2021

Postmates
Medium
ID 2015

36

Data Engineer
Data Scientist
BI Analyst
Data Analyst
ML Engineer
What cities recorded the largest growth and biggest drop in order amount between March 11, 2019, and April 11, 2019. Just compare order amounts on those two dates. Your output should include the names of the cities and the amount of growth/drop.

Tables: postmates_orders, postmates_markets


postmates_orders
Preview
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



postmates_markets
Preview
id	name	timezone
43	Boston	EST
44	Seattle	PST
47	Denver	MST
49	Chicago	CST  

  
'''

with cte as (
    select 
        pm.name as name,
        date(po.order_timestamp_utc),
        lag(sum(po.amount),1) over (partition by pm.name order by date(po.order_timestamp_utc) desc) 
        - sum(po.amount) as growth_difference
        from postmates_orders po inner join postmates_markets pm
        on po.city_id= pm.id
        where date(po.order_timestamp_utc) between '2019-03-11' and '2019-04-11'
        group by 1,2)
    select 
    name,
    growth_difference
    from
    cte where growth_difference = 
        (select max(growth_difference) from cte)
        or 
              growth_difference = 
        (select min(growth_difference) from cte);

'''
Your Solution:
name	growth_difference
Boston	-530.26
Seattle	192.74

'''
