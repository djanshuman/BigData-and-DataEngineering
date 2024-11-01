'''
https://platform.stratascratch.com/coding/2001-share-of-loan-balance?code_type=1
'''

select
    s1.rate_type,
    s1.loan_id,
    s1.balance,
    (s1.balance/total_balance)*100 as balance_share
    from submissions s1
left outer join
    (select
        rate_type,
        sum(balance) as total_balance
        from submissions
        group by rate_type
        ) s2 on s1.rate_type= s2.rate_type
group by 
    s1.rate_type,
    s1.loan_id,
    s1.balance,
    s2.total_balance;
