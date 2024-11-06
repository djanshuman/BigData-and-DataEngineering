'''
https://platform.stratascratch.com/coding/2014-hour-with-the-highest-order-volume?code_type=1


Hour With The Highest Order Volume


Interview Question Date: December 2020

Postmates
Medium
ID 2014

31

Data Engineer
Data Scientist
BI Analyst
Data Analyst
ML Engineer
Software Engineer
Which hour has the highest average order volume per day? Your output should have the hour which satisfies that condition, and average order volume.

Table: postmates_orders
Hints
Expected Output
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
id:
int
customer_id:
int
courier_id:
int
seller_id:
int
order_timestamp_utc:
datetime
amount:
float
city_id:
int
  
'''

with summary as (
select 
    order_hour,
    avg(n_orders) as avg_order
    from (
        select
        date_part('hour',order_timestamp_utc) as order_hour,
        date(order_timestamp_utc) as order_date,
        count(id) as n_orders
        from postmates_orders
        group by order_hour,order_date) a
group by order_hour)
select 
    * 
    from summary 
    where avg_order =
            (select max(avg_order) from summary
                ) ;


'''
Output
View the output in a separate browser tab
Execution time: 0.00950 seconds

Your Solution:
order_hour	avg_order
3	2
8	2
'''
