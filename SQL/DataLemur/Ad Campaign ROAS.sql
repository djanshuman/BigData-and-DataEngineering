'''
  https://datalemur.com/questions/ad-campaign-roas
Ad Campaign ROAS
Google SQL Interview Question
Question
Solution
Discussion
Submissions
Google marketing managers are analyzing the performance of various advertising accounts over the last month. They need your help to gather the relevant data.

Write a query to calculate the return on ad spend (ROAS) for each advertiser across all ad campaigns. Round your answer to 2 decimal places, and order your output by the advertiser_id.

Hint: ROAS = Ad Revenue / Ad Spend

ad_campaigns Table:

Column Name	Type
campaign_id	integer
spend	integer
revenue	float
advertiser_id	integer
ad_campaigns Example Input:

campaign_id	spend	revenue	advertiser_id
1	5000	7500	3
2	1000	900	1
3	3000	12000	2
4	500	2000	4
5	100	400	4
Example Output:

advertiser_id	ROAS
1	0.9
2	4
3	1.5
4	4
Explanation

The example output shows that advertiser_id 1 returned 90% of their ad spend, advertiser_id 2 returned 400% of their ad spend, and so on.

The dataset you are querying against may have different input & output - this is just an example!
'''

SELECT 
  advertiser_id,
  ROUND(CAST(sum(revenue) AS NUMERIC)/ sum(spend),2) AS ROAS
FROM ad_campaigns
group by 1
ORDER BY 1;


'''
Output
advertiser_id	roas
6	2.40
1	0.84
3	1.50
4	4.00
2	3.89

'''
