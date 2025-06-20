'''
https://platform.stratascratch.com/coding/2164-stock-codes-with-prices-above-average?code_type=1
You are given a dataset of online transactions, and your task is to identify product codes whose unit prices are greater than the average unit price of sold products.


•   The unit price should be the original price (i.e., the minimum unit price for each product code).
•   The average unit price should be computed based on the unique product codes and their original prices.


Your output should contain productcode and unitprice (the original price).


'''

select
    productcode,
    unitprice
    from online_retails
    where unitprice >
    (
        select
        avg(unitprice)
        from online_retails
    );

'''
Output
View the output in a separate browser tab
Execution time: 0.01041 seconds

Your Solution:
productcode	unitprice
23008	16.95
21625	6.95
22483	5.79
23356	5.95
21906	13.29
21155	4.96
22725	3.75
21169	4.13
22423	12.75
22193	8.5
21106	5.79
22960	4.25
'''
