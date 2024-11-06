'''
https://platform.stratascratch.com/coding/10172-best-selling-item?code_type=1

Find the best selling item for each month (no need to separate months by year) where the biggest total invoice was paid. 
The best selling item is calculated using the formula (unitprice * quantity). Output the month, the description of the item along with the amount paid.

  online_retail

invoiceno	stockcode	description	quantity	invoicedate	unitprice	customerid	country
544586	21890	S/6 WOODEN SKITTLES IN COTTON BAG	3	2011-02-21	2.95	17338	United Kingdom
541104	84509G	SET OF 4 FAIRY CAKE PLACEMATS	3	2011-01-13	3.29		United Kingdom
560772	22499	WOODEN UNION JACK BUNTING	3	2011-07-20	4.96		United Kingdom
555150	22488	NATURAL SLATE RECTANGLE CHALKBOARD	5	2011-05-31	3.29		United Kingdom
570521	21625	VINTAGE UNION JACK APRON	3	2011-10-11	6.95	12371	Switzerland
547053	22087	PAPER BUNTING WHITE LACE	40	2011-03-20	2.55	13001	United Kingdom
573360	22591	CARDHOLDER GINGHAM CHRISTMAS TREE	6	2011-10-30	3.25	15748	United Kingdom
571039	84536A	ENGLISH ROSE NOTEBOOK A7 SIZE	1	2011-10-13	0.42	16121	United Kingdom
578936	20723	STRAWBERRY CHARLOTTE BAG	10	2011-11-27	0.85	16923	United Kingdom
559338	21391	FRENCH LAVENDER SCENT HEART	1	2011-07-07	1.63		United Kingdom
568134	23171	REGENCY TEA PLATE GREEN	1	2011-09-23	3.29		United Kingdom
552061	21876	POTTERING MUG	12	2011-05-06	1.25	13001	United Kingdom
543179	22531	MAGIC DRAWING SLATE CIRCUS PARADE	1	2011-02-04	0.42	12754	Japan
540954	22381	TOY TIDY PINK POLKADOT	4	2011-01-12	2.1	14606	United Kingdom
572703	21818	GLITTER HEART DECORATION	13	2011-10-25	0.39	16110	United Kingdom
578757	23009	I LOVE LONDON BABY GIFT SET	1	2011-11-25	16.95	12748	United Kingdom

'''

select 
    month,
    description,
    total_paid
from (
select 
        date_part('month',invoicedate) as month,
        description,
        sum(unitprice * quantity) as total_paid,
        rank() over (partition by date_part('month',invoicedate) order by sum(unitprice * quantity) desc) as rank
    from online_retail
    group by 1,2
    order by 1) a where rank =1 order by month;

'''
Your Solution:
month	description	total_paid
1	LUNCH BAG SPACEBOY DESIGN	74.26
2	REGENCY CAKESTAND 3 TIER	38.25
3	PAPER BUNTING WHITE LACE	102
4	SPACEBOY LUNCH BOX	23.4
5	PAPER BUNTING WHITE LACE	51
6	Dotcomgiftshop Gift Voucher £50.00	41.67
7	PAPER BUNTING WHITE LACE	56.1
8	LUNCH BAG PINK POLKADOT	16.5
9	RED RETROSPOT PEG BAG	34.72
10	CHOCOLATE HOT WATER BOTTLE	102
11	RED WOOLLY HOTTIE WHITE HEART.	228.25
12	PAPER BUNTING RETROSPOT	35.4
'''
