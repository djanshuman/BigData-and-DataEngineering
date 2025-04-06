"""
  https://platform.stratascratch.com/coding/10300-premium-vs-freemium?code_type=1
  Find the total number of downloads for paying and non-paying users by date. Include only records where non-paying customers have more downloads than paying customers. 
  The output should be sorted by earliest date first and contain 3 columns date, non-paying downloads, paying downloads. Hint: In Oracle you should use "date" when referring to date column (reserved keyword).
  
"""

with downloads_total as (
select
    date,
    sum(case 
        when paying_customer ='no' then downloads end) as "non_paying",
    sum(case 
        when paying_customer = 'yes' then downloads end) as "paying"
    from ms_user_dimension inner join ms_acc_dimension
    on ms_user_dimension.acc_id=ms_acc_dimension.acc_id
    inner join ms_download_facts on
    ms_user_dimension.user_id = ms_download_facts.user_id
    group by date)
    select
        date,
        non_paying,
        paying
        from downloads_total
        where non_paying > paying
        order by date;
