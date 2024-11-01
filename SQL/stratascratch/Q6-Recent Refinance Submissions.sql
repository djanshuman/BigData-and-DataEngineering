'''
https://platform.stratascratch.com/coding/2003-recent-refinance-submissions?code_type=1
'''

with cte as(
select 
    user_id,
    type,
    balance,
    created_at,
    rank() over (partition by user_id order by created_at desc) as rnk
    from loans inner join submissions
    on loans.id = submissions.loan_id
    where type = 'Refinance'
    group by
    user_id,
    type,
    created_at,
    balance)
        select 
            user_id,
            sum(balance) as total_balance
            from cte where rnk =1
            group by user_id;
