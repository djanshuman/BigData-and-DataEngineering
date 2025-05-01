"""
https://datalemur.com/questions/page-recommendation

Write a query to recommend a page to a user. A recommendation is based on a page liked by user friends. Assume you have two tables: a two-column table of users and their friends, and a two-column table of users and the pages they liked.

Assumptions:

Only recommend the top page to the user, and do not recommend pages that were already liked by the user.
Top page is defined as the page with the highest number of followers.
Output the user id and page recommended. Order the result in ascending order by user id.

friendship Table:
Column Name	Type
id	integer
user_id	string
friend_id	string
friendship Example Input:
id	user_id	friend_id
1	alice	bob
2	alice	charles
3	alice	david
page_following Table:
Column Name	Type
id	integer
user_id	string
page_id	string
  
page_following Example Input:
id	user_id	page_id
1	alice	google
2	alice	facebook
3	bob	google
4	bob	linkedin
5	bob	facebook
  
Example Output:
user_id	Page_Recommended
alice	linkedin
Alice's friend Bob is following Google, Linkedin, and Facebook pages. However, since Alice is already following Google and Facebook, the only page that can be recommended to her is Linkedin.

The dataset you are querying against may have different input & output - this is just an example!
"""

with two_way_friendship as (
    SELECT user_id, friend_id
    FROM friendship
      UNION
    SELECT friend_id, user_id
    FROM friendship

),recommended_pages as (
SELECT
  friends.user_id,
  pages.page_id,
  count(*) as "followers"
  from two_way_friendship as friends
  left outer join 
  page_following as pages
  on friends.friend_id = pages.user_id
  where not exists(
        select id 
        from page_following as pages2
        where friends.user_id = pages2.user_id
        and pages.page_id = pages2.page_id
    )
  group by friends.user_id,pages.page_id

),top_pages as(
SELECT
  user_id,
  page_id,
  followers,
  dense_rank() over(partition by user_id order by followers desc) as "rnk"
  from recommended_pages)
  SELECT
    user_id,
    page_id
    from top_pages where rnk=1;

"""
Output

user_id	page_id
alice	linkedin
bob	github
charles	facebook
david	google
david	facebook

"""
