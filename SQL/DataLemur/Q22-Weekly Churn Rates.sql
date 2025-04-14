"""
https://datalemur.com/questions/first-month-retention

Facebook is analyzing its user signup data for June 2022. Write a query to generate the churn rate by week in June 2022. Output the week number (1, 2, 3, 4, ...) and the corresponding churn rate rounded to 2 decimal places.

For example, week number 1 represents the dates from 30 May to 5 Jun, and week 2 is from 6 Jun to 12 Jun.

Assumptions:

If the last_login date is within 28 days of the signup_date, the user can be considered churned.
If the last_login is more than 28 days after the signup date, the user didn't churn.
users Table:
Column Name	Type
user_id	integer
signup_date	datetime
last_login	datetime
  
users Example Input:
user_id	signup_date	last_login
1001	06/01/2022 12:00:00	07/05/2022 12:00:00
1002	06/03/2022 12:00:00	06/15/2022 12:00:00
1004	06/02/2022 12:00:00	06/15/2022 12:00:00
1006	06/15/2022 12:00:00	06/27/2022 12:00:00
1012	06/16/2022 12:00:00	07/22/2022 12:00:00
Example Output:
signup_week	churn_rate
1	66.67
3	50.00
User ids 1001, 1002, and 1004 signed up in the first week of June 2022. Out of the 3 users, 1002 and 1004's last login is within 28 days from the signup date, hence they are churned users.

To calculate the churn rate, we take churned users divided by total users signup in the week. Hence 2 users / 3 users = 66.67%.

The dataset you are querying against may have different input & output - this is just an example!
"""
with june22_churn as (
SELECT
    (EXTRACT(WEEK from signup_date) - 
    EXTRACT(WEEK from DATE_TRUNC('Month', signup_date))) + 1 as "signup_week",
    case 
    when last_login between signup_date and signup_date + interval '28 days' then '1'
    else NULL 
    end as "churned_users",
    count(user_id) over (partition by EXTRACT(WEEK from signup_date)) as "total_user"
    from users
    WHERE EXTRACT(MONTH FROM signup_date) = 6
      AND EXTRACT(YEAR FROM signup_date) = 2022)
  SELECT 
  signup_week,
    round(100.0*count(churned_users)/count(total_user),2) as "churn_rate"
    from june22_churn
    group by 1;

"""
  Output

signup_week	churn_rate
1	75.00
2	80.00
3	66.67
4	50.00

"""
