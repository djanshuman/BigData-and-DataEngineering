'''
https://platform.stratascratch.com/coding/2000-variable-vs-fixed-rates?code_type=1
'''

select 
loan_id,
count(  case 
            when (rate_type = 'fixed') then id
            else null
            end) as fixed,
count(case
            when (rate_type = 'variable') then id
            else null
            end) as VARIABLE
from submissions
group by loan_id;
