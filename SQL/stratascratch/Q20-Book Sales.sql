'''

https://platform.stratascratch.com/coding/2128-book-sales?code_type=1

Calculate the total revenue made per book. Output the book ID and total sales per book. In case there is a book that has never been sold, include it in your output with a value of 0.

amazon_books
  
book_id	book_title	unit_price
B001	The Hunger Games	25
B002	The Outsiders	50
B003	To Kill a Mockingbird	100
B004	Pride and Prejudice	20
B005	Twilight	30
B006	The Book Thief	50
B007	Animal Farm	40
B008	The Chronicles of Narnia	30
B009	Fahrenheit 451	50
B010	Gone with the Wind	25
B011	The Fault in Our Stars	30
B012	The Lightning Thief	35
B013	The Giving Tree	40
B014	Wuthering Heights	45
B015	The Da Vinci Code	60
B016	Memoirs of a Geisha	100
B017	Life of Pi	80
B018	The Little Prince	55
B019	Jane Eyre	60
B020	The Pillars of the Earth	60


amazon_books_order_details

order_details_id	order_id	book_id	quantity
OD101	O1001	B001	1
OD102	O1001	B009	1
OD103	O1002	B012	2
OD104	O1002	B006	1
OD105	O1002	B019	2
OD106	O1003	B017	1
OD107	O1004	B020	2
OD108	O1005	B020	2
OD109	O1006	B005	1
OD110	O1006	B001	1
OD111	O1007	B002	2
OD112	O1007	B003	1
OD113	O1008	B020	1
OD114	O1009	B006	1
OD115	O1010	B007	2
OD116	O1011	B019	1
OD117	O1011	B003	1
OD118	O1012	B006	3
OD119	O1013	B011	2
OD120	O1013	B001	2
OD121	O1014	B011	4
OD122	O1014	B012	1
OD123	O1014	B006	1
OD124	O1015	B010	2
OD125	O1016	B013	2
OD126	O1017	B012	1
OD127	O1017	B013	2
OD128	O1018	B016	3
OD129	O1019	B017	2
OD130	O1020	B009	1
  
'''


select 
    ab.book_id,
    coalesce(sum(ab.unit_price * abod.quantity),0) as revenue
from  amazon_books ab 
left outer join 
amazon_books_order_details abod on ab.book_id = abod.book_id
group by 1;

'''
Your Solution:
book_id	revenue
B007	80
B018	0
B009	100
B019	180
B015	0
B017	240
B013	160
B011	180
B020	300
B004	0
B002	100
B012	140
B003	200
B014	0
B008	0
B016	300
B006	300
B001	100
B010	50
B005	30
'''
