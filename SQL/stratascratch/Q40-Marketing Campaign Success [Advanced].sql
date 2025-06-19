'''
https://platform.stratascratch.com/coding/514-marketing-campaign-success-advanced?code_type=1


Marketing Campaign Success [Advanced]


Last Updated: June 2025

Hard
ID 514

272

About This Question
You have the marketing_campaign table, which records in-app purchases by users. Users making their first in-app purchase enter a marketing campaign, where they see call-to-actions for more purchases. Find how many users made additional purchases due to the campaigns success.

The campaign starts one day after the first purchase. Users with only one or multiple purchases on the first day do not count, nor do users who later buy only the same products from their first day.


'''

with first_purchase as (
select 
    user_id,
    min(created_at) as "first_pur_dt"
    from marketing_campaign
    group by 1),
    
valid_users as (
select
    user_id
    from marketing_campaign
    group by 1
    having count(distinct created_at) > 1
    and count(distinct product_id) > 1),
    
first_purchase_detail as (
select
    mc.user_id,
    mc.product_id
    from marketing_campaign mc inner join first_purchase fp
    on mc.user_id = fp.user_id
    where mc.created_at = fp.first_pur_dt)
    
select
    count(distinct mc.user_id)
    from marketing_campaign mc inner join valid_users vu
    on mc.user_id = vu.user_id
    left outer join first_purchase_detail fpd 
    on mc.user_id = fpd.user_id
    and mc.product_id = fpd.product_id
    where fpd.product_id is null;

'''
Your Solution:
count
23
'''
    
  
