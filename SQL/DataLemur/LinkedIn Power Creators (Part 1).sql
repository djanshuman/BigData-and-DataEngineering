'''
  LinkedIn Power Creators (Part 1)
LinkedIn SQL Interview Question
Question
Solution
Discussion
Submissions
The LinkedIn Creator team is seeking out individuals who have a strong influence on the platform, utilizing their personal profiles as a company or influencer page. To identify such power creators, we can compare the number of followers on their LinkedIn page with the number of followers on the company they work for. If a persons LinkedIn page has more followers than their company, we consider them to be a power creator.

Write a query to retrieve the profile IDs of these LinkedIn power creators ordered in ascending order based on their IDs.

Assumptions:

Each individual in the database has a LinkedIn profile associated with a single company.
The personal_profiles and company_pages tables are connected via the employer_id and company_id columns, respectively.
personal_profiles Table:

Column Name	Type
profile_id	integer
name	string
followers	integer
employer_id	integer
personal_profiles Example Input:

profile_id	name	followers	employer_id
1	Nick Singh	92,000	4
2	Zach Wilson	199,000	2
3	Daliana Liu	171,000	1
4	Ravit Jain	107,000	3
5	Vin Vashishta	139,000	6
6	Susan Wojcicki	39,000	5
company_pages Table:

Column Name	Type
company_id	integer
name	string
followers	integer
company_pages Example Input:

company_id	name	followers
1	The Data Science Podcast	8,000
2	Airbnb	700,000
3	The Ravit Show	6,000
4	DataLemur	200
5	YouTube	1,6000,000
6	DataScience.Vin	4,500
Example Output:

profile_id
1
3
4
5
This output indicates that profile IDs 1-5 are power creators, having more followers than their company pages.

The dataset you are querying against may have different input & output - this is just an example!
'''



SELECT 
  distinct profile_id
  FROM personal_profiles a inner join company_pages b 
  on a.employer_id = b.company_id
  where a.followers > b.followers
  order by profile_id;



'''
profile_id
1
3
4
5
7
'''
