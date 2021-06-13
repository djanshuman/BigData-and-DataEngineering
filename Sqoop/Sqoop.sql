sqoop-list-databases \
--connect "jdbc:mysql://quickstart.cloudera:3306" \
--username retail_dba \
--password cloudera


sqoop-list-tables \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera

sqoop-eval \
--connect "jdbc:mysql://quickstart.cloudera:3306" \
--username retail_dba \
--password cloudera \
--query "select * from retail_db.categories limit 10"


create database sqooplearning;

use sqooplearning;


create table people(
Person_id int,
Lastname varchar(255),
firstname varchar(255),
Address varchar(255),
city varchar(255));

Insert into people values (100,’Nayak’,’Dibyajyoti’,’Bhubaneswar’,’India’);
Insert into people values (101,Das,Ashish,Cuttack,Odisha);
Insert into people values (102,Jena,Akash,Balasore,’India’);
Commit;

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/sqooplearning \
--username root \
--password cloudera \
--table people \
--m 1 \
--target-dir /queryresult


[cloudera@quickstart ~]$ hadoop fs -ls /queryresult
Found 2 items
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 03:18 /queryresult/_SUCCESS
-rw-r--r--   1 cloudera supergroup     	99 2021-06-13 03:18 /queryresult/part-m-00000
[cloudera@quickstart ~]$ hadoop fs -cat /queryresult/*
102,Jena,Akash,Balasore,India
101,Das,Ashish,Cuttack,Odisha
100,Nayak,Dibyajyoti,Bhubaneswar,India
[cloudera@quickstart ~]$


sqoop import-all-tables \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--as-sequencefile \
--warehouse-dir /alltables

[cloudera@quickstart ~]$ hadoop fs -ls /alltables
Found 3 items
drwxr-xr-x   - cloudera supergroup      	0 2021-06-13 03:27 /alltables/categories
drwxr-xr-x   - cloudera supergroup      	0 2021-06-13 03:28 /alltables/customers
drwxr-xr-x   - cloudera supergroup      	0 2021-06-13 03:29 /alltables/departments
[cloudera@quickstart ~]$



sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table orders \
--password cloudera \
--as-sequencefile \
--warehouse-dir /alltables

[cloudera@quickstart ~]$ hadoop fs -ls /alltables/orders
Found 5 items
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 03:34 /alltables/orders/_SUCCESS
-rw-r--r--   1 cloudera supergroup 	880159 2021-06-13 03:34 /alltables/orders/part-m-00000
-rw-r--r--   1 cloudera supergroup 	880420 2021-06-13 03:34 /alltables/orders/part-m-00001
-rw-r--r--   1 cloudera supergroup 	879621 2021-06-13 03:34 /alltables/orders/part-m-00002
-rw-r--r--   1 cloudera supergroup 	880255 2021-06-13 03:34 /alltables/orders/part-m-00003

[cloudera@quickstart ~]$ hadoop fs -cat  /alltables/orders/* | head
SEQ!org.apache.hadoop.io.LongWritableorders�ƣ��|�z,o�-��@���-OCLOSED@���PENDING_PAYMENT@���/COMPLETE@���"{CLOSED@���,COMPLETE@���COMPLETE@���COMPLET@���
                                                          	_
PROCESSING0 	@���PENDING_PAYMENT0
@���PENDING_PAYMENT/


sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table orders \
--password cloudera \
--warehouse-dir /alltables

[cloudera@quickstart ~]$ hadoop fs -cat  /alltables/orders/* | head1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
3,2013-07-25 00:00:00.0,12111,COMPLETE
4,2013-07-25 00:00:00.0,8827,CLOSED
5,2013-07-25 00:00:00.0,11318,COMPLETE
6,2013-07-25 00:00:00.0,7130,COMPLETE
7,2013-07-25 00:00:00.0,4530,COMPLETE
8,2013-07-25 00:00:00.0,2911,PROCESSING
9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT
10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT

--as-sequencefile
--as-avrodatafile
--as-parquetfile
--as-textfile

sqoop eval \
--connect jdbc:mysql://quickstart.cloudera:3306/ \
--username root \
--password cloudera \
--e ‘select * from retail_db.orders limit 10’

21/06/13 03:46:23 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
--------------------------------------------------------------------------
| order_id	| order_date      	| order_customer_id | order_status     	|
--------------------------------------------------------------------------
| 1       	| 2013-07-25 00:00:00.0 | 11599   	| CLOSED           	|
| 2       	| 2013-07-25 00:00:00.0 | 256     	| PENDING_PAYMENT  	|
| 3       	| 2013-07-25 00:00:00.0 | 12111   	| COMPLETE         	|
| 4       	| 2013-07-25 00:00:00.0 | 8827    	| CLOSED           	|
| 5       	| 2013-07-25 00:00:00.0 | 11318   	| COMPLETE         	|
| 6       	| 2013-07-25 00:00:00.0 | 7130    	| COMPLETE         	|
| 7       	| 2013-07-25 00:00:00.0 | 4530    	| COMPLETE         	|
| 8       	| 2013-07-25 00:00:00.0 | 2911    	| PROCESSING       	|
| 9       	| 2013-07-25 00:00:00.0 | 5657    	| PENDING_PAYMENT  	|
| 10      	| 2013-07-25 00:00:00.0 | 5648    	| PENDING_PAYMENT  	|
--------------------------------------------------------------------------

sqoop eval \
--connect jdbc:mysql://quickstart.cloudera:3306/ \
--username root \
--password cloudera \
--e 'select * from retail_db.orders limit 10' 1>query.out 2>err.out

[cloudera@quickstart ~]$ cat err.out
21/06/13 03:48:58 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
21/06/13 03:48:58 WARN tool.BaseSqoopTool: Setting your password on the command-line is insecure. Consider using -P instead.
21/06/13 03:48:58 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
[cloudera@quickstart ~]$ cat query.out
Warning: /usr/lib/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
--------------------------------------------------------------------------
| order_id	| order_date      	| order_customer_id | order_status     	|
--------------------------------------------------------------------------
| 1       	| 2013-07-25 00:00:00.0 | 11599   	| CLOSED           	|
| 2       	| 2013-07-25 00:00:00.0 | 256     	| PENDING_PAYMENT  	|
| 3       	| 2013-07-25 00:00:00.0 | 12111   	| COMPLETE         	|
| 4       	| 2013-07-25 00:00:00.0 | 8827    	| CLOSED           	|
| 5       	| 2013-07-25 00:00:00.0 | 11318   	| COMPLETE         	|
| 6       	| 2013-07-25 00:00:00.0 | 7130    	| COMPLETE         	|
| 7       	| 2013-07-25 00:00:00.0 | 4530    	| COMPLETE         	|
| 8       	| 2013-07-25 00:00:00.0 | 2911    	| PROCESSING       	|
| 9       	| 2013-07-25 00:00:00.0 | 5657    	| PENDING_PAYMENT  	|
| 10      	| 2013-07-25 00:00:00.0 | 5648    	| PENDING_PAYMENT  	|
--------------------------------------------------------------------------
[cloudera@quickstart ~]$


sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table orders \
--password cloudera \
--compress \
--warehouse-dir /alltables

[cloudera@quickstart ~]$ hadoop fs -ls /alltables/ordersFound 5 items
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 03:53 /alltables/orders/_SUCCESS
-rw-r--r--   1 cloudera supergroup 	116403 2021-06-13 03:53 /alltables/orders/part-m-00000.gz
-rw-r--r--   1 cloudera supergroup 	116338 2021-06-13 03:53 /alltables/orders/part-m-00001.gz
-rw-r--r--   1 cloudera supergroup 	116612 2021-06-13 03:53 /alltables/orders/part-m-00002.gz
-rw-r--r--   1 cloudera supergroup 	122334 2021-06-13 03:53 /alltables/orders/part-m-00003.gz
[cloudera@quickstart ~]$

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table orders \
--password cloudera \
--compression-codec BZip2Codec \
--warehouse-dir /alltables

[cloudera@quickstart ~]$ hadoop fs -ls /alltables/orders
Found 5 items
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 03:59 /alltables/orders/_SUCCESS
-rw-r--r--   1 cloudera supergroup  	93167 2021-06-13 03:59 /alltables/orders/part-m-00000.bz2
-rw-r--r--   1 cloudera supergroup  	95745 2021-06-13 03:59 /alltables/orders/part-m-00001.bz2
-rw-r--r--   1 cloudera supergroup  	96684 2021-06-13 03:59 /alltables/orders/part-m-00002.bz2
-rw-r--r--   1 cloudera supergroup 	100584 2021-06-13 03:59 /alltables/orders/part-m-00003.bz2


sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table orders \
--password cloudera \
--columns order_id,order_status \
--where "order_status in ('complete','closed')" \
--warehouse-dir /alltables

    	
21/06/13 04:06:05 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`order_id`), MAX(`order_id`) FROM `orders` WHERE ( order_status in ('complete','closed') )
21/06/13 04:06:06 INFO db.IntegerSplitter: Split size: 17220; Num splits: 4 from: 1 to: 68883

Bytes Written=436799

21/06/13 04:07:05 INFO mapreduce.ImportJobBase: Transferred 426.5615 KB in 64.1129 seconds (6.6533 KB/sec)
21/06/13 04:07:05 INFO mapreduce.ImportJobBase: Retrieved 30455 records.

[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders/part-m-* | wc -l
30455
[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders/part-m-* | head
1,CLOSED
3,COMPLETE
4,CLOSED
5,COMPLETE
6,COMPLETE


Source table:

mysql> select count(*) from retail_db.orders where order_status in ('complete','closed');
+----------+
| count(*) |
+----------+
|	30455 |
+----------+
1 row in set (0.05 sec)


mysql> insert into retail_db.orders values(200000,current_timestamp(),9999,'Complete');
commit;
|	68880 | 2014-07-13 00:00:00 |          	1117 | COMPLETE    	|
|	68881 | 2014-07-19 00:00:00 |          	2518 | PENDING_PAYMENT |
|	68882 | 2014-07-22 00:00:00 |         	10000 | ON_HOLD     	|
|	68883 | 2014-07-23 00:00:00 |          	5533 | COMPLETE    	|
|   200000 | 2021-06-13 04:24:10 |          	9999 | Complete    	|
+----------+---------------------+-------------------+-----------------+
68884 rows in set (0.06 sec)




sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table orders \
--password cloudera \
--columns order_id,order_status \
--where "order_status in ('complete','closed')" \
--warehouse-dir /alltables

21/06/13 04:27:38 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`order_id`), MAX(`order_id`) FROM `orders` WHERE ( order_status in ('complete','closed') )
21/06/13 04:27:38 INFO db.IntegerSplitter: Split size: 49999; Num splits: 4 from: 1 to: 200000
21/06/13 04:27:39 WARN hdfs.DFSClient: Caught exception

    	Bytes Written=436815
21/06/13 04:28:57 INFO mapreduce.ImportJobBase: Transferred 426.5771 KB in 88.4119 seconds (4.8249 KB/sec)
21/06/13 04:28:57 INFO mapreduce.ImportJobBase: Retrieved 30456 records.
[cloudera@quickstart ~]$

[cloudera@quickstart ~]$ hadoop fs -ls /alltables/orders/*
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 04:28 /alltables/orders/_SUCCESS
-rw-r--r--   1 cloudera supergroup 	316904 2021-06-13 04:28 /alltables/orders/part-m-00000
-rw-r--r--   1 cloudera supergroup 	119895 2021-06-13 04:28 /alltables/orders/part-m-00001
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 04:28 /alltables/orders/part-m-00002
-rw-r--r--   1 cloudera supergroup     	16 2021-06-13 04:28 /alltables/orders/part-m-00003

[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders/part-m-00000 | wc -l
22188
[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders/part-m-00001 | wc -l
8267
[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders/part-m-00002 | wc -l
0
[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders/part-m-00003 | wc -l
1
[cloudera@quickstart ~]$








sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table orders \
--password cloudera \
--columns order_id,order_status \
--where "order_status in ('complete','closed')" \
--boundary-query "select 1,68883" \
--warehouse-dir /alltables

21/06/13 04:38:38 INFO db.DBInputFormat: Using read commited transaction isolation
21/06/13 04:38:38 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: select 1,68883
21/06/13 04:38:38 INFO db.IntegerSplitter: Split size: 17220; Num splits: 4 from: 1 to: 68883


21/06/13 04:39:35 INFO mapreduce.ImportJobBase: Transferred 426.5615 KB in 59.4654 seconds (7.1733 KB/sec)
21/06/13 04:39:35 INFO mapreduce.ImportJobBase: Retrieved 30455 records.

[cloudera@quickstart ~]$ hadoop fs -ls /alltables/orders/*
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 04:39 /alltables/orders/_SUCCESS
-rw-r--r--   1 cloudera supergroup 	106081 2021-06-13 04:39 /alltables/orders/part-m-00000
-rw-r--r--   1 cloudera supergroup 	109971 2021-06-13 04:39 /alltables/orders/part-m-00001
-rw-r--r--   1 cloudera supergroup 	111238 2021-06-13 04:39 /alltables/orders/part-m-00002
-rw-r--r--   1 cloudera supergroup 	109509 2021-06-13 04:39 /alltables/orders/part-m-00003
[cloudera@quickstart ~]$


[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders/part-m-00003 | tail
68859,COMPLETE
68863,CLOSED
68870,COMPLETE
68872,COMPLETE
68874,COMPLETE
68876,COMPLETE
68878,COMPLETE
68879,COMPLETE
68880,COMPLETE
68883,COMPLETE
[cloudera@quickstart ~]$




sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table order_items \
--password cloudera \
--columns order_item_id,order_item_order_id \
--boundary-query "select min(order_item_order_id),max(order_item_order_id) from order_items where order_item_id > 10000" \
--warehouse-dir /alltables

21/06/13 04:54:22 INFO db.DBInputFormat: Using read commited transaction isolation
21/06/13 04:54:22 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: select min(order_item_order_id),max(order_item_order_id) from order_items where order_item_id > 10000
21/06/13 04:54:22 INFO db.IntegerSplitter: Split size: 16214; Num splits: 4 from: 4024 to: 68883
    	Bytes Written=751393
21/06/13 04:55:32 INFO mapreduce.ImportJobBase: Transferred 733.7822 KB in 73.7203 seconds (9.9536 KB/sec)
21/06/13 04:55:32 INFO mapreduce.ImportJobBase: Retrieved 64860 records.

[cloudera@quickstart ~]$ hadoop fs -cat /alltables/order_items/part-m-00000 | head
4024,1614
4025,1614
4026,1614
4027,1614
4028,1615
4029,1615
4030,1615
4031,1615
4032,1616
4033,1617

[cloudera@quickstart ~]$ hadoop fs -cat /alltables/order_items/part-m-00003 | tail
68874,27517
68875,27520
68876,27520
68877,27520
68878,27520
68879,27521
68880,27521
68881,27521
68882,27521
68883,27522




Source
mysql> select order_item_id ,order_item_order_id from order_items where order_item_order_id in (4024,68883);
+---------------+---------------------+
| order_item_id | order_item_order_id |
+---------------+---------------------+
|      	9999 |            	4024 |
|     	10000 |            	4024 |
|     	10001 |            	4024 |
|     	10002 |            	4024 |
|     	10003 |            	4024 |
|    	172197 |           	68883 |
|    	172198 |           	68883 |
+---------------+---------------------+
7 rows in set (0.06 sec)

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table order_items \
--password cloudera \
--columns order_item_id,order_item_order_id \
--boundary-query "select min(order_item_order_id),max(order_item_order_id) from order_items" --where "order_item_id > 10000" \
--warehouse-dir /alltables

21/06/13 05:12:30 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: select min(order_item_order_id),max(order_item_order_id) from order_items
21/06/13 05:12:30 INFO db.IntegerSplitter: Split size: 17220; Num splits: 4 from: 1 to: 68883
21/06/13 05:12:30 INFO mapreduce.JobSubmitter: number of splits:4

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table order_items \
--password cloudera \
--columns order_item_id,order_item_order_id \
--boundary-query "select min(order_item_id),max(order_item_id) from order_items  where order_item_id > 10000" \
--warehouse-dir /alltables

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table order_items \
--password cloudera \
--columns order_item_id,order_item_order_id \
--boundary-query "select min(order_item_order_id),max(order_item_order_id) from order_items" \
--where "order_item_id > 10000" \
--warehouse-dir /alltables


21/06/13 05:28:53 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: select min(order_item_order_id),max(order_item_order_id) from order_items


sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table order_items \
--password cloudera \
--columns order_item_id,order_item_order_id \
--where "order_item_id > 10000" \
--boundary-query "select min(order_item_order_id),max(order_item_order_id) from order_items" \
--warehouse-dir /alltables

21/06/13 05:31:37 INFO db.DBInputFormat: Using read commited transaction isolation
21/06/13 05:31:37 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: select min(order_item_order_id),max(order_item_order_id) from order_items


[cloudera@quickstart ~]$ hadoop fs -cat /alltables/order_items/part-m-00000 | head
10001,4024
10002,4024
10003,4024
10004,4025
10005,4025
10006,4025
10007,4025
10008,4026
10009,4026
10010,4027



sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--table order_items \
--password cloudera \
--columns order_item_id,order_item_order_id \
--boundary-query "select min(order_item_order_id),max(order_item_order_id) from order_items" \
--where "order_item_id > 10000" \
--warehouse-dir /alltables

21/06/13 05:35:26 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: select min(order_item_order_id),max(order_item_order_id) from order_items
21/06/13 05:35:26 INFO db.IntegerSplitter: Split size: 17220; Num splits: 4 from: 1 to: 68883
[cloudera@quickstart ~]$ hadoop fs -cat /alltables/order_items/part-m-00000 | head
10001,4024
10002,4024
10003,4024
10004,4025
10005,4025
10006,4025
10007,4025
10008,4026
10009,4026
10010,4027

Observations:
==========
If (--boundary-query) is provided and  (-- where) is provided in the sqoop import then it only includes the boundary query in the BoundingValsQuery and where clause is not seen in the terminal. But internally it is used to filter the dataset .
If (--boundary-query) is not  provided and only (--where) clause is provided then the where clause is also seen in the BoundingValsQuery along with the implicit min(p.k) and max(p.k) query that is prepared by sqoop framework. 
If we provide the (--boundary-query) string in sqoop import along with where clause as a single query statement then it takes the entire query string as a BoundingValsQuery and seen in the terminal. [ Applies for both p.k column or non p.k column used in the query string] 

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--warehouse-dir /alltables
21/06/13 05:53:17 ERROR tool.ImportTool: Import failed: No primary key could be found for table orders_no_pk. Please specify one with --split-by or perform a sequential import with '-m 1'.

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--num-mappers 1 \
--warehouse-dir /alltables

21/06/13 05:53:57 INFO db.DBInputFormat: Using read commited transaction isolation
21/06/13 05:53:57 INFO mapreduce.JobSubmitter: number of splits:1

[cloudera@quickstart ~]$ hadoop fs -ls /alltables/orders_no_pk/
Found 2 items
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 05:54 /alltables/orders_no_pk/_SUCCESS
-rw-r--r--   1 cloudera supergroup	2999944 2021-06-13 05:54 /alltables/orders_no_pk/part-m-00000

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--split-by order_id \
--warehouse-dir /alltables

21/06/13 05:57:07 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`order_id`), MAX(`order_id`) FROM `orders_no_pk`
21/06/13 05:57:07 INFO db.IntegerSplitter: Split size: 17220; Num splits: 4 from: 1 to: 68883

[cloudera@quickstart ~]$ hadoop fs -ls /alltables/orders_no_pk/
Found 5 items
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 05:58 /alltables/orders_no_pk/_SUCCESS
-rw-r--r--   1 cloudera supergroup 	741614 2021-06-13 05:58 /alltables/orders_no_pk/part-m-00000
-rw-r--r--   1 cloudera supergroup 	753022 2021-06-13 05:58 /alltables/orders_no_pk/part-m-00001
-rw-r--r--   1 cloudera supergroup 	752368 2021-06-13 05:58 /alltables/orders_no_pk/part-m-00002
-rw-r--r--   1 cloudera supergroup 	752940 2021-06-13 05:58 /alltables/orders_no_pk/part-m-00003

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--split-by order_id \
--num-mappers 1 \
--warehouse-dir /alltables
21/06/13 06:02:07 INFO mapreduce.JobSubmitter: number of splits:1

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--split-by order_id \
--num-mappers 5 \
--warehouse-dir /alltables

21/06/13 06:03:22 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`order_id`), MAX(`order_id`) FROM `orders_no_pk`
21/06/13 06:03:22 INFO db.IntegerSplitter: Split size: 13776; Num splits: 5 from: 1 to: 68883

Observations: We should use --num-mappers 1 / --m 1 only when if the table has no suitable non p.k column for which values division interval is consistent and outliers are not present.
Since the input split is set to 1 and only 1 mapper takes the load this should be avoided. Better to identify the column which can be used as a split by column as num of mappers will be >=4 and parallelism won’t be restricted.

String Splitter:
==========
sqoop import \
-Dorg.apache.sqoop.splitter.allow_text_splitter=true \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--split-by order_status \
--warehouse-dir /alltables \
--delete-target-dir

e6fd6c20097ee43/orders_no_pk.jar
21/06/13 06:16:00 INFO tool.ImportTool: Destination directory /alltables/orders_no_pk deleted.



21/06/13 06:13:00 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`order_status`), MAX(`order_status`) FROM `orders_no_pk`
21/06/13 06:13:00 WARN db.TextSplitter: Generating splits for a textual index column.
21/06/13 06:13:00 WARN db.TextSplitter: If your database sorts in a case-insensitive order, this may result in a partial import or duplicate records.
21/06/13 06:13:00 WARN db.TextSplitter: You are strongly encouraged to choose an integral split column.

Handling job failure for table when we don’t know whether source table has p.k or not.
It will reset to use 1 mapper in such case or if p.k exist use 8 as below:

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--warehouse-dir /alltables \
--autoreset-to-one-mapper \
--num-mappers 8 \
--delete-target-dir

Executing --autoreset-to-one-mapper on table which has p.k will result to default 4 mappers as --num-mappers is not explicitly provided.

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--warehouse-dir /alltables \
--autoreset-to-one-mapper \
--delete-target-dir
21/06/13 06:22:27 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`order_id`), MAX(`order_id`) FROM `orders`
21/06/13 06:22:27 INFO db.IntegerSplitter: Split size: 17220; Num splits: 4 from: 1 to: 68883








sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--warehouse-dir /alltables \
--fields-terminated-by '|' \
--lines-terminated-by '\n' \
--delete-target-dir
[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders/part-m-00000 | head
1|2013-07-25 00:00:00.0|11599|CLOSED
2|2013-07-25 00:00:00.0|256|PENDING_PAYMENT
3|2013-07-25 00:00:00.0|12111|COMPLETE
4|2013-07-25 00:00:00.0|8827|CLOSED
5|2013-07-25 00:00:00.0|11318|COMPLETE
6|2013-07-25 00:00:00.0|7130|COMPLETE
7|2013-07-25 00:00:00.0|4530|COMPLETE
8|2013-07-25 00:00:00.0|2911|PROCESSING
9|2013-07-25 00:00:00.0|5657|PENDING_PAYMENT
10|2013-07-25 00:00:00.0|5648|PENDING_PAYMENT

sqoop create-hive-table \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--hive-table orders_hive \
--fields-terminated-by '|' \
--lines-terminated-by '\n'

21/06/13 06:33:27 WARN hive.TableDefWriter: Column order_date had to be cast to a less precise type in Hive
21/06/13 06:33:30 INFO hive.HiveImport: Loading uploaded data into Hive

Logging initialized using configuration in jar:file:/usr/lib/hive/lib/hive-common-1.1.0-cdh5.13.0.jar!/hive-log4j.properties
OK
Time taken: 8.018 seconds
[cloudera@quickstart ~]$ hive

Logging initialized using configuration in file:/etc/hive/conf.dist/hive-log4j.properties
WARNING: Hive CLI is deprecated and migration to Beeline is recommended.
hive> show databases;
OK
default
Time taken: 0.94 seconds, Fetched: 1 row(s)
hive> show create table orders_hive;
OK
CREATE TABLE `orders_hive`(
  `order_id` int,
  `order_date` string,
  `order_customer_id` int,
  `order_status` string)
COMMENT 'Imported by sqoop on 2021/06/13 06:33:27'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'field.delim'='|',
  'line.delim'='\n',
  'serialization.format'='|')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://quickstart.cloudera:8020/user/hive/warehouse/orders_hive'
TBLPROPERTIES (
  'transient_lastDdlTime'='1623591220')
Time taken: 0.472 seconds, Fetched: 20 row(s)

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders \
--warehouse-dir /alltables \
--fields-terminated-by '|' \
--lines-terminated-by '\n' \
--append

21/06/13 06:44:16 INFO mapreduce.ImportJobBase: Transferred 2.861 MB in 77.7672 seconds (37.6718 KB/sec)
21/06/13 06:44:16 INFO mapreduce.ImportJobBase: Retrieved 68883 records.
21/06/13 06:44:16 INFO util.AppendUtils: Appending to directory orders
21/06/13 06:44:16 INFO util.AppendUtils: Using found partition 4
[cloudera@quickstart ~]$

[cloudera@quickstart ~]$ hadoop fs -ls /alltables/orders/
Found 9 items
-rw-r--r--   1 cloudera supergroup      	0 2021-06-13 06:29 /alltables/orders/_SUCCESS
-rw-r--r--   1 cloudera supergroup 	741614 2021-06-13 06:29 /alltables/orders/part-m-00000
-rw-r--r--   1 cloudera supergroup 	753022 2021-06-13 06:29 /alltables/orders/part-m-00001
-rw-r--r--   1 cloudera supergroup 	752368 2021-06-13 06:29 /alltables/orders/part-m-00002
-rw-r--r--   1 cloudera supergroup 	752940 2021-06-13 06:29 /alltables/orders/part-m-00003
-rw-r--r--   1 cloudera cloudera   	741614 2021-06-13 06:44 /alltables/orders/part-m-00004
-rw-r--r--   1 cloudera cloudera   	753022 2021-06-13 06:44 /alltables/orders/part-m-00005
-rw-r--r--   1 cloudera cloudera   	752368 2021-06-13 06:44 /alltables/orders/part-m-00006
-rw-r--r--   1 cloudera cloudera   	752940 2021-06-13 06:44 /alltables/orders/part-m-00007

Handling nulls for integer and string types:

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--warehouse-dir /alltables \
--autoreset-to-one-mapper \
--fields-terminated-by '|' \
--lines-terminated-by '\n' \
--null-string 'none' \
--null-non-string '-1' \
--delete-target-dir

[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders_no_pk/part-m-00000 | tail
68878|2014-07-08 00:00:00.0|6753|COMPLETE
68879|2014-07-09 00:00:00.0|778|COMPLETE
68880|2014-07-13 00:00:00.0|1117|COMPLETE
68881|2014-07-19 00:00:00.0|2518|PENDING_PAYMENT
68882|2014-07-22 00:00:00.0|10000|ON_HOLD
68883|2014-07-23 00:00:00.0|5533|COMPLETE
68884|2021-06-13 06:50:46.0|1023|CLOSED
68885|-1|10234|CLOSED
68886|-1|-1|PENDING
68887|2021-06-13 06:51:47.0|9099|none
[cloudera@quickstart ~]$

Source:
	68884 | 2021-06-13 06:50:46 |          	1023 | CLOSED      	|
|	68885 | NULL            	|         	10234 | CLOSED      	|
|	68886 | NULL            	|          	NULL | PENDING     	|
|	68887 | 2021-06-13 06:51:47 |          	9099 | NULL        	|
+----------+---------------------+-------------------+-----------------+
68887 rows in set (0.13 sec)


Sqoop export:

sqoop export \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_for_export \
--staging-table orders_for_export_stg \
--export-dir /alltables/orders_no_pk/ \
--fields-terminated-by '|'

Job failed : 

1/06/13 07:09:22 INFO mapreduce.ExportJobBase: Transferred 1.4386 MB in 72.8699 seconds (20.2164 KB/sec)
21/06/13 07:09:22 INFO mapreduce.ExportJobBase: Exported 34566 records.
21/06/13 07:09:22 ERROR tool.ExportTool: Error during export:
Export job failed!
mysql> select count(*) from orders_for_export_stg;
+----------+
| count(*) |
+----------+
|	68884 |
+----------+
1 row in set (0.02 sec)

68884|2021-06-13 06:50:46.0|1023|CLOSED
68885|-1|10234|CLOSED
68886|-1|-1|PENDING
68887|2021-06-13 06:51:47.0|9099|none

Job failed due to null values in dataset, checked in tracking URL , we can handle it with null-string and null-non-string.  But staging table has data loaded till correct id 68884. We will truncate the staging table and re-run the  export to load from core.

Caused by: java.lang.IllegalArgumentException: Timestamp format must be yyyy-mm-dd hh:mm:ss[.fffffffff]
	at java.sql.Timestamp.valueOf(Timestamp.java:202)
	at orders_for_export.__loadFromFields(orders_for_export.java:355)
	... 12 more


Only import Rows till 68884 . Post 68884 we have null values , so we will only export rows till 68884 to a new table in mysql.

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_no_pk \
--warehouse-dir /alltables \
--autoreset-to-one-mapper \
--fields-terminated-by '|' \
--lines-terminated-by '\n' \
--where 'order_id <=68884' \
--delete-target-dir

    	Bytes Written=2999984
21/06/13 07:30:17 INFO mapreduce.ImportJobBase: Transferred 2.861 MB in 39.0688 seconds (74.9875 KB/sec)
21/06/13 07:30:17 INFO mapreduce.ImportJobBase: Retrieved 68884 records.

[cloudera@quickstart ~]$ hadoop fs -cat /alltables/orders_no_pk/part-m-00000 | tail
68875|2014-07-04 00:00:00.0|10637|ON_HOLD
68876|2014-07-06 00:00:00.0|4124|COMPLETE
68877|2014-07-07 00:00:00.0|9692|ON_HOLD
68878|2014-07-08 00:00:00.0|6753|COMPLETE
68879|2014-07-09 00:00:00.0|778|COMPLETE
68880|2014-07-13 00:00:00.0|1117|COMPLETE
68881|2014-07-19 00:00:00.0|2518|PENDING_PAYMENT
68882|2014-07-22 00:00:00.0|10000|ON_HOLD
68883|2014-07-23 00:00:00.0|5533|COMPLETE
68884|2021-06-13 06:50:46.0|1023|CLOSED
[cloudera@quickstart ~]$

Now we will execute export for imported dataset to mysql to a new stage and final table.
sqoop export \
--connect jdbc:mysql://quickstart.cloudera:3306/retail_db \
--username root \
--password cloudera \
--table orders_for_export \
--staging-table orders_for_export_stg \
--export-dir /alltables/orders_no_pk/ \
--fields-terminated-by '|'
21/06/13 07:34:44 INFO mapreduce.ExportJobBase: Transferred 2.8733 MB in 85.8825 seconds (34.2592 KB/sec)
21/06/13 07:34:44 INFO mapreduce.ExportJobBase: Exported 68884 records.
21/06/13 07:34:44 INFO mapreduce.ExportJobBase: Starting to migrate data from staging table to destination.
21/06/13 07:34:45 INFO manager.SqlManager: Migrated 68884 records from `orders_for_export_stg` to `orders_for_export`
[cloudera@quickstart ~]$

mysql> select count(*) from orders_for_export_stg;
+----------+
| count(*) |
+----------+
|    	0 |
+----------+
1 row in set (0.00 sec)

mysql> select count(*) from orders_for_export;
+----------+
| count(*) |
+----------+
|	68884 |
+----------+
1 row in set (0.04 sec)

mysql>

SQOOP INCREMENTAL:
mysql> create database banking;
Query OK, 1 row affected (0.00 sec)

mysql> use banking;
Database changed

Create table member_details(
card_id BIGINT,
member_id BIGINT,
member_joining_dt timestamp,
card_purchase_dt varchar(255),
country varchar(255),
city varchar(255),
PRIMARY KEY(card_id));


mysql> create table member_details_stg as select * from member_details where 1=0;
Query OK, 0 rows affected (0.01 sec)
Records: 0  Duplicates: 0  Warnings: 0

mysql> desc member_details_stg;
+-------------------+--------------+------+-----+---------------------+-------+
| Field         	| Type     	| Null | Key | Default         	| Extra |
+-------------------+--------------+------+-----+---------------------+-------+
| card_id       	| bigint(20)   | NO   | 	| 0               	|   	|
| member_id     	| bigint(20)   | YES  | 	| NULL            	|   	|
| member_joining_dt | timestamp	| NO   | 	| 0000-00-00 00:00:00 |   	|
| card_purchase_dt  | varchar(255) | YES  | 	| NULL            	|   	|
| country       	| varchar(255) | YES  | 	| NULL            	|   	|
| city          	| varchar(255) | YES  | 	| NULL            	|   	|
+-------------------+--------------+------+-----+---------------------+-------+
6 rows in set (0.00 sec)

mysql>


cloudera@quickstart Downloads]$ hadoop fs -mkdir -p /anshuman/banking/card_member

[cloudera@quickstart Downloads]$ hadoop fs -put /home/cloudera/Downloads/cardmembers-201210-221319.csv /anshuman/banking/card_member/
[cloudera@quickstart Downloads]$ hadoop fs -mv  /anshuman/banking/card_member/cardmembers-201210-221319.csv /anshuman/banking/card_member/cardmembers.csv
hadoop[cloudera@quickstart Downloads]$ hadoop fs -ls /anshuman/banking/card_member/
Found 1 items
-rw-r--r--   1 cloudera supergroup  	85082 2021-06-13 08:16 /anshuman/banking/card_member/cardmembers.csv
[cloudera@quickstart Downloads]$

sqoop export \
--connect jdbc:mysql://quickstart.cloudera:3306/banking \
--username root \
--password cloudera \
--staging-table member_details_stg \
--table member_details \
--export-dir /anshuman/banking/card_member/cardmembers.csv \
--fields-terminated-by ','

06/13 08:22:46 INFO mapreduce.ExportJobBase: Transferred 99.8066 KB in 68.2094 seconds (1.4632 KB/sec)
21/06/13 08:22:46 INFO mapreduce.ExportJobBase: Exported 999 records.
21/06/13 08:22:46 INFO mapreduce.ExportJobBase: Starting to migrate data from staging table to destination.
21/06/13 08:22:46 INFO manager.SqlManager: Migrated 999 records from `member_details_stg` to `member_details`

Database changed
mysql> select count(*) from member_details;
+----------+
| count(*) |
+----------+
|  	999 |
+----------+
1 row in set (0.00 sec)

mysql>

Sqoop incremental load demo (manual way)

sqoop import \
--connect jdbc:mysql://quickstart.cloudera:3306/banking \
--username root \
--password cloudera \
--table member_details \
--warehouse-dir /anshuman/banking/card_member \
--incremental append \
--check-column card_id \
--last-value 0

 Log:
BoundingValsQuery: SELECT MIN(`card_id`), MAX(`card_id`) FROM `member_details` WHERE ( `card_id` > 0 AND `card_id` <= 6599900931314251 )

06/13 08:44:34 INFO mapreduce.ImportJobBase: Transferred 82.9785 KB in 76.819 seconds (1.0802 KB/sec)
21/06/13 08:44:34 INFO mapreduce.ImportJobBase: Retrieved 999 records.
21/06/13 08:44:34 INFO util.AppendUtils: Creating missing output directory - member_details
21/06/13 08:44:34 INFO tool.ImportTool: Incremental import complete! To run another incremental import of all data following this import, supply the following arguments:
21/06/13 08:44:34 INFO tool.ImportTool:  --incremental append
21/06/13 08:44:34 INFO tool.ImportTool:   --check-column card_id
21/06/13 08:44:34 INFO tool.ImportTool:   --last-value 6599900931314251
21/06/13 08:44:34 INFO tool.ImportTool: (Consider saving this with 'sqoop job --create')
[cloudera@quickstart Downloads]$

[cloudera@quickstart Downloads]$ hadoop fs -ls /anshuman/banking/card_member/member_details
Found 4 items
-rw-r--r--   1 cloudera cloudera  	23037 2021-06-13 08:44 /anshuman/banking/card_member/member_details/part-m-00000
-rw-r--r--   1 cloudera cloudera      	0 2021-06-13 08:44 /anshuman/banking/card_member/member_details/part-m-00001
-rw-r--r--   1 cloudera cloudera  	20663 2021-06-13 08:44 /anshuman/banking/card_member/member_details/part-m-00002
-rw-r--r--   1 cloudera cloudera  	41270 2021-06-13 08:44 /anshuman/banking/card_member/member_details/part-m-00003
[cloudera@quickstart Downloads]$ hadoop fs -cat /anshuman/banking/card_member/member_details/part-m-00003 | tail
6590907016354002,849289511810887,2016-10-28 07:32:23.0,07/17,United States,Reading
6591175617713393,135615957283320,2015-05-16 03:21:15.0,08/16,United States,Arvin
6592184145413632,714853788321318,2010-03-12 08:11:17.0,08/16,United States,Mehlville
6594248319343442,846162645575666,2016-02-15 02:32:58.0,12/16,United States,Lyndhurst
6595638658736751,377918213614639,2015-08-13 10:58:24.0,09/17,United States,Mahwah
6595814135833988,236864426408837,2011-02-18 06:45:46.0,06/14,United States,Johnston
6595928469079750,422173403853722,2012-02-10 02:19:20.0,02/14,United States,Raleigh
6597703848279563,657991752750272,2017-02-16 07:45:34.0,03/17,United States,Pinewood
6598830758632447,660350842993890,2013-07-26 02:22:17.0,12/16,United States,Tampa
6599900931314251,928036864799687,2017-09-09 03:31:54.0,10/17,United States,Loves Park
[cloudera@quickstart Downloads]$


Sqoop Job Automated way:
=======================
sqoop job \
--create job_banking_member_details \
-- import  \
--connect jdbc:mysql://quickstart.cloudera:3306/banking \
--username root \
--password cloudera \
--table member_details \
--warehouse-dir /anshuman/banking/card_member \
--incremental append \
--check-column card_id \
--last-value 0


[cloudera@quickstart Downloads]$ sqoop job --list
Warning: /usr/lib/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
21/06/13 09:23:05 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
Available jobs:
  job_banking_member_details
[cloudera@quickstart Downloads]$


sqoop job --exec job_banking_member_details

    	Bytes Written=84970
21/06/13 09:28:58 INFO mapreduce.ImportJobBase: Transferred 82.9785 KB in 68.8603 seconds (1.205 KB/sec)
21/06/13 09:28:58 INFO mapreduce.ImportJobBase: Retrieved 999 records.
21/06/13 09:28:58 INFO util.AppendUtils: Creating missing output directory - member_details
21/06/13 09:28:58 INFO tool.ImportTool: Saving incremental import state to the metastore
21/06/13 09:28:59 INFO tool.ImportTool: Updated data for job: job_banking_member_details
[cloudera@quickstart Downloads]$

[cloudera@quickstart Downloads]$ hadoop fs -ls /anshuman/banking/card_member/member_details/
Found 4 items
-rw-r--r--   1 cloudera cloudera  	23037 2021-06-13 09:28 /anshuman/banking/card_member/member_details/part-m-00000
-rw-r--r--   1 cloudera cloudera      	0 2021-06-13 09:28 /anshuman/banking/card_member/member_details/part-m-00001
-rw-r--r--   1 cloudera cloudera  	20663 2021-06-13 09:28 /anshuman/banking/card_member/member_details/part-m-00002
-rw-r--r--   1 cloudera cloudera  	41270 2021-06-13 09:28 /anshuman/banking/card_member/member_details/part-m-00003
[cloudera@quickstart Downloads]$


Sqoop job stores the metadata in local inside a hidden folder .sqoop. It contains two metadata file metastore.db.properties, metastore.db.script.

[cloudera@quickstart ~]$ ls -lart .sqoop/
total 20
drwxrwxr-x 28 cloudera cloudera 4096 Jun 13 09:22 ..
-rw-rw-r--  1 cloudera cloudera 7434 Jun 13 09:28 metastore.db.script
-rw-rw-r--  1 cloudera cloudera  419 Jun 13 09:28 metastore.db.properties
drwxrwxr-x  2 cloudera cloudera 4096 Jun 13 09:28 .
[cloudera@quickstart ~]$ pwd
/home/cloudera
[cloudera@quickstart ~]$
[cloudera@quickstart ~]$ cat .sqoop/metastore.db.script |head -15
CREATE SCHEMA PUBLIC AUTHORIZATION DBA
CREATE MEMORY TABLE SQOOP_ROOT(VERSION INTEGER,PROPNAME VARCHAR(128) NOT NULL,PROPVAL VARCHAR(256),CONSTRAINT SQOOP_ROOT_UNQ UNIQUE(VERSION,PROPNAME))
CREATE MEMORY TABLE SQOOP_SESSIONS(JOB_NAME VARCHAR(64) NOT NULL,PROPNAME VARCHAR(128) NOT NULL,PROPVAL VARCHAR(1024),PROPCLASS VARCHAR(32) NOT NULL,CONSTRAINT SQOOP_SESSIONS_UNQ UNIQUE(JOB_NAME,PROPNAME,PROPCLASS))
CREATE USER SA PASSWORD ""
GRANT DBA TO SA
SET WRITE_DELAY 10
SET SCHEMA PUBLIC
INSERT INTO SQOOP_ROOT VALUES(NULL,'sqoop.hsqldb.job.storage.version','0')
INSERT INTO SQOOP_ROOT VALUES(0,'sqoop.hsqldb.job.info.table','SQOOP_SESSIONS')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','sqoop.tool','import','schema')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','sqoop.property.set.id','0','schema')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','verbose','false','SqoopOptions')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','hcatalog.drop.and.create.table','false','SqoopOptions')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','incremental.last.value','6599900931314251','SqoopOptions')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','db.connect.string','jdbc:mysql://quickstart.cloudera:3306/banking','SqoopOptions')

[cloudera@quickstart ~]$ cat .sqoop/metastore.db.properties |head -15
#HSQL Database Engine 1.8.0.10
#Sun Jun 13 09:28:59 PDT 2021
hsqldb.script_format=0
runtime.gc_interval=0
sql.enforce_strict_size=false
hsqldb.cache_size_scale=8
readonly=false
hsqldb.nio_data_file=true
hsqldb.cache_scale=14
version=1.8.0
hsqldb.default_table_type=memory
hsqldb.cache_file_scale=1
hsqldb.log_size=200
modified=no
hsqldb.cache_version=1.7.0
[cloudera@quickstart ~]$


Sqoop password creation for password management:
=====================================
4 ways to pass password in sqoop

Pass -P for password : Enter password on prompt.
--password in the script , hardcode password .
Pass the password-file:
[cloudera@quickstart ~]$ echo -n "cloudera" >> .password-file
[cloudera@quickstart ~]$ cat .password-file
cloudera[cloudera@quickstart ~]$

sqoop job \
--create job_banking_member_details \
-- import  \
--connect jdbc:mysql://quickstart.cloudera:3306/banking \
--username root \
--password-file file:///home/cloudera/.password-file \
--table member_details \
--warehouse-dir /anshuman/banking/card_member \
--incremental append \
--check-column card_id \
--last-value 0

21/06/13 09:50:35 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
[cloudera@quickstart ~]$
[cloudera@quickstart ~]$ sqoop job --list
Warning: /usr/lib/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
21/06/13 09:50:52 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
Available jobs:
  job_banking_member_details
[cloudera@quickstart ~]$ sqoop job -exec job_banking_member_details
Warning: /usr/lib/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
21/06/13 09:51:06 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
21/06/13 09:51:10 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
21/06/13 09:51:10 INFO tool.CodeGenTool: Beginning code generation


Password alias method: it uses jceks i.e java cryptography encryption keystore for complete encryption of password and it is unreadable.


[cloudera@quickstart ~]$ hadoop credential create mysql.banking.password -provider jceks://hdfs/user/cloudera/mysql.password.jceks
WARNING: You have accepted the use of the default provider password
by not configuring a password in one of the two following locations:
	* In the environment variable HADOOP_CREDSTORE_PASSWORD
	* In a file referred to by the configuration entry
  	hadoop.security.credstore.java-keystore-provider.password-file.
Please review the documentation regarding provider passwords in
the keystore passwords section of the Credential Provider API
Continuing with the default provider password.

Enter alias password:
Enter alias password again:
mysql.banking.password has been successfully created.
Provider jceks://hdfs/user/cloudera/mysql.password.jceks has been updated.
[cloudera@quickstart ~]$

[cloudera@quickstart ~]$ hadoop fs -cat /user/cloudera/mysql.password.jceks
����mysql.banking.passwordzQ�q��sr3com.sun.crypto.provider.SealedObjectForKeyProtector�W�Y�0�Sxrjavax.crypto.SealedObject>6=�÷m_�2Q�uq~�>�lx�L�&�a{uk(Z��h�u��7��031��r�&���2�);Rz݊�m#���2�yØ���&�M������<j�Y�b33гkst�E�_P����b�?�n���ҡ]���� ���G��� �?��f�}�itPBEWithMD5AndTripleDEStPBEWithMD5AndTripleDES�2�9s>�
                                                           	�0�؄9+P[cloudera@quickstart ~]$
[cloudera@quickstart ~]$
[cloudera@quickstart ~]$

sqoop job \
-Dhadoop.security.credential.provider.path=jceks://hdfs/user/cloudera/mysql.password.jceks \
--create job_banking_member_details \
-- import  \
--connect jdbc:mysql://quickstart.cloudera:3306/banking \
--username root \
--password-alias mysql.banking.password \
--table member_details \
--warehouse-dir /anshuman/banking/card_member \
--incremental append \
--check-column card_id \
--last-value 0


[cloudera@quickstart ~]$ sqoop job -exec job_banking_member_details
Warning: /usr/lib/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
21/06/13 10:08:39 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0
21/06/13 10:08:48 INFO manager.MySQLManager: Preparing to use a MySQL streaming resultset.
21/06/13 10:08:48 INFO tool.CodeGenTool: Beginning code generation
21/06/13 10:08:48 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM `member_details` AS t LIMIT 1
21/06/13 10:08:48 INFO manager.SqlManager: Executing SQL statement: SELECT t.* FROM `member_details` AS t LIMIT 1
21/06/13 10:08:48 INFO orm.CompilationManager: HADOOP_MAPRED_HOME is /usr/lib/hadoop-mapreduce

Sqoop incremental last_modfiied 
=======================
sqoop job \
-Dhadoop.security.credential.provider.path=jceks://hdfs/user/cloudera/mysql.password.jceks \
--create job_banking_member_details \
-- import  \
--connect jdbc:mysql://quickstart.cloudera:3306/banking \
--username root \
--password-alias mysql.banking.password \
--table member_details \
--warehouse-dir /anshuman/banking/card_member \
--incremental lastmodified \
--check-column member_joining_dt \
--last-value 0

cloudera@quickstart ~]$ sqoop job --exec job_banking_member_details
Warning: /usr/lib/sqoop/../accumulo does not exist! Accumulo imports will fail.
Please set $ACCUMULO_HOME to the root of your Accumulo installation.
21/06/13 10:17:57 INFO sqoop.Sqoop: Running Sqoop version: 1.4.6-cdh5.13.0

21/06/13 10:18:12 INFO db.DBInputFormat: Using read commited transaction isolation
21/06/13 10:18:12 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`card_id`), MAX(`card_id`) FROM `member_details` WHERE ( `member_joining_dt` >= '0' AND `member_joining_dt` < '2021-06-13 10:18:09.0' )
21/06/13 10:18:12 INFO db.IntegerSplitter: Split size: 1564968116401259; Num splits: 4 from: 340028465709212 to: 6599900931314251
21/0

21/06/13 10:19:17 INFO mapreduce.ImportJobBase: Retrieved 999 records.
21/06/13 10:19:17 INFO tool.ImportTool: Final destination exists, will run merge job.
21/06/13 10:19:17 INFO tool.ImportTool: Moving data from temporary directory _sqoop/b854c35da6fe4bd791eb259eb48cbf87_member_details to final destination /anshuman/banking/card_member/member_details
21/06/13 10:19:17 INFO tool.ImportTool: Saving incremental import state to the metastore
21/06/13 10:19:17 INFO tool.ImportTool: Updated data for job: job_banking_member_details

[cloudera@quickstart ~]$ hadoop fs -ls /anshuman/banking/card_member/member_details
Found 5 items
-rw-r--r--   1 cloudera cloudera      	0 2021-06-13 10:19 /anshuman/banking/card_member/member_details/_SUCCESS
-rw-r--r--   1 cloudera cloudera  	23037 2021-06-13 10:19 /anshuman/banking/card_member/member_details/part-m-00000
-rw-r--r--   1 cloudera cloudera      	0 2021-06-13 10:19 /anshuman/banking/card_member/member_details/part-m-00001
-rw-r--r--   1 cloudera cloudera  	20663 2021-06-13 10:19 /anshuman/banking/card_member/member_details/part-m-00002
-rw-r--r--   1 cloudera cloudera  	41270 2021-06-13 10:19 /anshuman/banking/card_member/member_details/part-m-00003
[cloudera@quickstart ~]$

[cloudera@quickstart ~]$ cat .sqoop/metastore.db.script | head -15
CREATE SCHEMA PUBLIC AUTHORIZATION DBA
CREATE MEMORY TABLE SQOOP_ROOT(VERSION INTEGER,PROPNAME VARCHAR(128) NOT NULL,PROPVAL VARCHAR(256),CONSTRAINT SQOOP_ROOT_UNQ UNIQUE(VERSION,PROPNAME))
CREATE MEMORY TABLE SQOOP_SESSIONS(JOB_NAME VARCHAR(64) NOT NULL,PROPNAME VARCHAR(128) NOT NULL,PROPVAL VARCHAR(1024),PROPCLASS VARCHAR(32) NOT NULL,CONSTRAINT SQOOP_SESSIONS_UNQ UNIQUE(JOB_NAME,PROPNAME,PROPCLASS))
CREATE USER SA PASSWORD ""
GRANT DBA TO SA
SET WRITE_DELAY 10
SET SCHEMA PUBLIC
INSERT INTO SQOOP_ROOT VALUES(NULL,'sqoop.hsqldb.job.storage.version','0')
INSERT INTO SQOOP_ROOT VALUES(0,'sqoop.hsqldb.job.info.table','SQOOP_SESSIONS')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','sqoop.tool','import','schema')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','sqoop.property.set.id','0','schema')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','verbose','false','SqoopOptions')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','hcatalog.drop.and.create.table','false','SqoopOptions')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','incremental.last.value','2021-06-13 10:18:09.0','SqoopOptions')
INSERT INTO SQOOP_SESSIONS VALUES('job_banking_member_details','db.connect.string','jdbc:mysql://quickstart.cloudera:3306/banking','SqoopOptions')


Now the output directory exists and files are present based on last job run with lastvalue updated in metastore. Now if we re-run the job it should run and pull data from last updated lastvalue but
It will fail because the directory exists. So in incremental append mode we don’t expect update on old records so if we use --append with job then new files will be added to the directory but no duplicate will be there as incremental append mode only have new inserts. 
Now in incremental lastmodified mode we can expect update + new record so since the directly exists using append with lastmodifed mode can lead to duplicates if update has come for old record. So we will be using --merge-key p.k with lastmodified mode , it will flatten up the previous and new files and only keep updated rows.

cloudera@quickstart ~]$ sqoop job --exec job_banking_member_details
Warning: /usr/lib/sqoop/../accumulo does not exist! Accumulo imports will fail.
_details.jar
21/06/13 10:23:52 ERROR tool.ImportTool: Import failed: --merge-key or --append is required when using --incremental lastmodified and the output directory exists.
[cloudera@quickstart ~]$

sqoop job \
-Dhadoop.security.credential.provider.path=jceks://hdfs/user/cloudera/mysql.password.jceks \
--create job_banking_member_details \
-- import  \
--connect jdbc:mysql://quickstart.cloudera:3306/banking \
--username root \
--password-alias mysql.banking.password \
--table member_details \
--warehouse-dir /anshuman/banking/card_member \
--incremental lastmodified \
--check-column member_joining_dt \
--last-value 0 \
--merge-key card_id
21/06/13 10:37:33 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`card_id`), MAX(`card_id`) FROM `member_details` WHERE ( `member_joining_dt` >= '0' AND `member_joining_dt` < '2021-06-13 10:37:30.0' )

21/06/13 10:40:22 INFO tool.ImportTool: Saving incremental import state to the metastore
21/06/13 10:40:23 INFO tool.ImportTool: Updated data for job: job_banking_member_details
[cloudera@quickstart ~]$

[cloudera@quickstart ~]$ hadoop fs  -cat /anshuman/banking/card_member/member_details/part-r-00000 | tail
6592184145413632,714853788321318,2010-03-12 08:11:17.0,08/16,United States,Mehlville
6594248319343442,846162645575666,2016-02-15 02:32:58.0,12/16,United States,Lyndhurst
6595638658736751,377918213614639,2015-08-13 10:58:24.0,09/17,United States,Mahwah
6595814135833988,236864426408837,2011-02-18 06:45:46.0,06/14,United States,Johnston
6595928469079750,422173403853722,2012-02-10 02:19:20.0,02/14,United States,Raleigh
6597703848279563,657991752750272,2017-02-16 07:45:34.0,03/17,United States,Pinewood
6598830758632447,660350842993890,2013-07-26 02:22:17.0,12/16,United States,Tampa
6599900931314251,928036864799687,2021-06-13 10:35:31.0,10/17,United States,Goa
7378373284723,19493434001,2021-06-13 10:33:47.0,04/14,India,Balasore
8893,550239,2021-06-13 10:34:31.0,04/20,India,Cuttack
[cloudera@quickstart ~]$


We have inserted and updated few rows:
1002 rows in set (0.00 sec)

mysql> insert into member_details values (1212,333401,current_timestamp(),'03/15','India','Pune');
Query OK, 1 row affected (0.02 sec)

mysql> insert into member_details values (0992943438434,4544545,current_timestamp(),'01/12','India','Kolkata');
Query OK, 1 row affected (0.00 sec)

mysql> update member_details set city='Santa Clara' where card_id=6595638658736751;
Query OK, 1 row affected (0.01 sec)
Rows matched: 1  Changed: 1  Warnings: 0
mysql> commit;
Query OK, 0 rows affected (0.00 sec)

Now we will re-run the job


cloudera@quickstart ~]$ sqoop job --exec job_banking_member_details
21/06/13 10:45:32 INFO db.DBInputFormat: Using read commited transaction isolation
21/06/13 10:45:32 INFO db.DataDrivenDBInputFormat: BoundingValsQuery: SELECT MIN(`card_id`), MAX(`card_id`) FROM `member_details` WHERE ( `member_joining_dt` >= '2021-06-13 10:37:30.0' AND `member_joining_dt` < '2021-06-13 10:45:29.0' )
21/06/13 10:45:32 INFO db.IntegerSplitter: Split size: 1648909664683884; Num splits: 4 from: 1212 to: 6595638658736751
21/06/13 10:46:22 INFO mapreduce.ImportJobBase: Transferred 201 bytes in 52.7686 seconds (3.8091 bytes/sec)
21/06/13 10:46:22 INFO mapreduce.ImportJobBase: Retrieved 3 records.
21/06/13 10:46:22 INFO tool.ImportTool: Final destination exists, will run merge job.

21/06/13 10:46:25 INFO mapreduce.Job: Running job: job_1622483378494_0052
21/06/13 10:46:39 INFO mapreduce.Job: Job job_1622483378494_0052 running in uber mode : false
21/06/13 10:46:39 INFO mapreduce.Job:  map 0% reduce 0%
21/06/13 10:47:18 INFO mapreduce.Job:  map 40% reduce 0%
21/06/13 10:47:20 INFO mapreduce.Job:  map 100% reduce 0%
21/06/13 10:47:31 INFO mapreduce.Job:  map 100% reduce 100%
21/06/13 10:47:32 INFO mapreduce.Job: Job job_1622483378494_0052 completed successfully
21/06/13 10:47:32 INFO mapreduce.Job: Counters: 50
    File System Counters


First records are retrieved using import then merge tool run reducer to aggregate into a single file only the latest record for the p.k

[cloudera@quickstart ~]$ hadoop fs  -cat /anshuman/banking/card_member/member_details/part-r-00000 | tail
6594248319343442,846162645575666,2016-02-15 02:32:58.0,12/16,United States,Lyndhurst
6595638658736751,377918213614639,2021-06-13 10:43:00.0,09/17,United States,Santa Clara
6595814135833988,236864426408837,2011-02-18 06:45:46.0,06/14,United States,Johnston
6595928469079750,422173403853722,2012-02-10 02:19:20.0,02/14,United States,Raleigh
6597703848279563,657991752750272,2017-02-16 07:45:34.0,03/17,United States,Pinewood
6598830758632447,660350842993890,2013-07-26 02:22:17.0,12/16,United States,Tampa
6599900931314251,928036864799687,2021-06-13 10:35:31.0,10/17,United States,Goa
7378373284723,19493434001,2021-06-13 10:33:47.0,04/14,India,Balasore
8893,550239,2021-06-13 10:34:31.0,04/20,India,Cuttack
992943438434,4544545,2021-06-13 10:42:36.0,01/12,India,Kolkata
[cloudera@quickstart ~]$




