'''
https://platform.stratascratch.com/coding/10141-apple-product-counts?code_type=1

We are exploring user data for a platform to see how popular Apple devices are among our users. We want to understand this popularity in the context of different languages. 
Analyze the data and show us the number of people using Apple devices compared to the total number of users, broken down by language.


The following devices should be considered in your analysis: "macbook pro", "iphone 5s", and "ipad air". 
Present the results showing the language, the number of Apple users, and the total number of users for each language. Finally, arrange the results so the languages with the most overall users are at the top

'''

--solution--

select pu.language,
    count( distinct case 
                        when pe.device in ('macbook pro',
                                            'iphone 5s',
                                            'ipad air') then pe.user_id
                        else null
                    end) AS "n_apple_users",
          count(distinct pe.user_id) as "n_total_users"
    from playbook_events pe inner join playbook_users pu on pe.user_id = pu.user_id
    group by pu.language
    order by n_total_users desc;
    
