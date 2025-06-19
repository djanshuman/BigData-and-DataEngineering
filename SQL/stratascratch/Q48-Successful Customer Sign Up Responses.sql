'''
https://platform.stratascratch.com/coding/2152-successful-customer-sign-up-responses?code_type=1

Its time to find out who is the top employee. Youve been tasked with finding the employee (or employees, in the case of a tie) who have received the most votes.
A vote is recorded when a customer leaves their 10-digit phone number in the free text customer_response column of their sign up response (occurrence of any number sequence with exactly 10 digits is considered as a phone number)

Output the top employee and the number of customer responses that left a number.

'''
with cte as (
select 
    employee_id,
    count(REGEXP_MATCH(customer_response,'[0-9]{10}')) as "cust_numbers",
    dense_rank() over(
        order by count(REGEXP_MATCH(customer_response,'[0-9]{10}')) desc
    ) as "rnk"
    from customer_responses
    group by 1)
select 
    employee_id,
    cust_numbers
    from cte where rnk=1;

'''
Your Solution:
employee_id	cust_numbers
1001	3
1006	3
'''
