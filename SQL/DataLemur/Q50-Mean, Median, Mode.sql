"""
https://datalemur.com/questions/mean-median-mode
You're given a list of numbers representing the number of emails in the inbox of Microsoft Outlook users. Before the Product Management team can start developing features related to bulk-deleting email or achieving inbox zero, they simply want to find the mean, median, and mode for the emails.

Display the output of mean, median and mode (in this order), with the mean rounded to the nearest integer. It should be assumed that there are no ties for the mode.

inbox_stats Table:
Column Name	Type
user_id	integer
email_count	integer
inbox_stats Example Input:
user_id	email_count
123	100
234	200
345	300
456	200
567	200
Example Output:
mean	median	mode
200	200	200
Explanation
The mean is calculated by adding up all the email counts and dividing by the number of users, resulting in a mean of 200 (i.e., (100 + 200 + 300 + 200 + 200) / 5).

The mode is the value that occurs most frequently, which is 200 in this case, since it appears three times, more than any other value.

The median is the middle value of the ordered dataset. When the data is arranged in order from smallest to largest (100, 200, 200, 200, 300), the median is also 200, which separates the lower half from the higher half of the values.

The dataset you are querying against may have different input & output - this is just an example!
"""

'''
  Solution 
Clean solution without using built-in function. Also handles odd and even cases. Note : If question asks for exact median value present in dataset then below changes are needed, avg() not required and (total_count + 1) / 2 handles both odd and even cases.

  median_cte as (
select 
   email_count as median 
   from ranked
   where rn in (
      (total_count + 1) / 2 
   )),
  
  '''
  

Current Question solution


-- Getting interpolated median --
with ranked as (
SELECT 
  email_count,
  row_number () over (order by email_count) as rn,
  count(*) over () as total_count
  FROM inbox_stats),

median_cte as (
select 
   ROUND(avg(email_count),0) as median -- for even, avg of middle two values
   from ranked
   where rn in (
      (total_count + 1) / 2 , -- odd number of elements. In case of odd, both values will be same
      (total_count + 2) / 2 -- even number of elements 
   )),
-- Getting mode --
mode_cte as(
select 
      email_count as mode
      from  (
SELECT 
  email_count,
  rank () over (order by count(*) desc) as rn
  FROM inbox_stats
  group by email_count) a where rn = 1)

-- final select --

SELECT
  ROUND(avg(email_count),0) AS MEAN,
  (SELECT median from median_cte),
  (SELECT mode from mode_cte)
  from inbox_stats;


'''
  Output
mean	median	mode
298	175	200
  '''
