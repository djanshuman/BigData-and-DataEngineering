'''
https://platform.stratascratch.com/coding/2095-three-purchases?code_type=1

List the IDs of customers who made at least 3 orders in both 2020 and 2021.

id	user_id	order_date	order_total
1383	U201	2020-06-06	100.31
1394	U203	2021-04-23	12.36
1380	U206	2020-01-15	13.44
1406	U201	2021-12-31	14.51
1404	U203	2021-09-17	15.17
1381	U205	2020-03-29	15.89
1378	U201	2019-12-31	16.6
1407	U204	2022-01-08	16.66
1375	U204	2018-05-24	21.18
1390	U205	2020-10-31	22.85
1403	U203	2021-07-29	24.96
1387	U206	2020-08-28	27.66
1405	U205	2021-12-01	27.74
1386	U205	2020-08-28	3.88
1392	U206	2020-12-13	30.82
1385	U203	2020-06-12	35.32
1398	U202	2021-07-04	37.11
1395	U205	2021-04-27	37.77
1389	U201	2020-10-17	40.19
1384	U203	2020-06-12	40.9
1377	U204	2019-05-25	41.69
1401	U202	2021-07-05	41.88
1379	U203	2020-01-01	46.92
1396	U203	2021-05-15	51.39
1402	U205	2021-07-22	53.92
1376	U204	2018-12-12	58.91
1397	U202	2021-07-03	59.24
1399	U202	2021-07-05	6.56
1374	U204	2017-01-03	63.43
1382	U205	2020-03-29	64.36
1388	U206	2020-09-12	64.54
1400	U202	2021-07-05	77.81
1391	U205	2020-10-31	78.67
1408	U205	2022-02-26	88.73
1393	U201	2021-03-01	90.81
'''

with cte as (
select 
    user_id
    from amazon_orders
    where extract(year from order_date) = '2020'
    group by user_id
    having count(*) >=3)
    select 
        user_id 
        from cte 
        where user_id in (
                select 
                user_id
                from amazon_orders
                where extract(year from order_date) = '2021'
                group by user_id
                having count(*) >=3
        );

'''
  
user_id
U203
U205
'''