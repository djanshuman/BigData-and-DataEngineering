"""
  https://datalemur.com/questions/marketing-touch-streak
  As a Data Analyst on Snowflake's Marketing Analytics team, your objective is to analyze customer relationship management (CRM) data and identify contacts that satisfy two conditions:

Contacts who had a marketing touch for three or more consecutive weeks.
Contacts who had at least one marketing touch of the type 'trial_request'.
Marketing touches, also known as touch points, represent the interactions or points of contact between a brand and its customers.

Your goal is to generate a list of email addresses for these contacts.

marketing_touches Table:
Column Name	Type
event_id	integer
contact_id	integer
event_type	string ('webinar', 'conference_registration', 'trial_request')
event_date	date

  
marketing_touches Example Input:
event_id	contact_id	event_type	event_date
1	1	webinar	4/17/2022
2	1	trial_request	4/23/2022
3	1	whitepaper_download	4/30/2022
4	2	handson_lab	4/19/2022
5	2	trial_request	4/23/2022
6	2	conference_registration	4/24/2022
7	3	whitepaper_download	4/30/2022
8	4	trial_request	4/30/2022
9	4	webinar	5/14/2022
crm_contacts Table:
Column Name	Type
contact_id	integer
email	string
  
crm_contacts Example Input:
contact_id	email
1	andy.markus@att.net
2	rajan.bhatt@capitalone.com
3	lissa_rogers@jetblue.com
4	kevinliu@square.com
  
Example Output:
email
andy.markus@att.net
Explanation:
Among the contacts, only Contact ID 1 (andy.markus@att.net) satisfies both conditions specified in the problem. Contact ID 1 had a marketing touch with an event type of 'trial_request' and the marketing touch points occurred consecutively over a period of 3 weeks. This meets both conditions of having a marketing touch for three or more consecutive weeks.

On the other hand, Contact ID 2 (rajan.bhatt@capitalone.com) is not included in the generated list. Although they had a marketing touch with the event type 'trial_request', their touch points took place within the same week. Consequently, this does not meet the requirement of consecutive marketing touches for 3 weeks.

The dataset you are querying against may have different input & output - this is just an example!
"""

with cte as (
select 
  contact_id,
  lag(date_trunc('week',event_date)) over(
  partition by contact_id order by date_trunc('week',event_date)
  rows between unbounded preceding and current row) as "previos_week_start",
  date_trunc('week',event_date) as "event_week",
  lead(date_trunc('week',event_date)) over(
  partition by contact_id order by date_trunc('week',event_date)
  rows between unbounded preceding and current row) as "next_week_start"
  from marketing_touches),
  cte_2 as (
  SELECT  
    * from cte 
    where previos_week_start = event_week - interval '1 week'
    OR
    next_week_start = event_week + interval '1 week'
    and contact_id in (
          select contact_id
          from marketing_touches where event_type = 'trial_request'
      ))
  SELECT
  distinct 
  c2.email
  from crm_contacts c2 inner join cte_2 as c1
  on c1.contact_id = c2.contact_id;

"""
Output

email
andy.markus@att.net
john.doe@datascience.com
"""
