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

"""
Explanation:

Solution
Step 1: Establish a two-way relationship between the user and their friends

Our first step is to establish a two-way relationship between the user and their friends. You might be wondering, what does that even mean?

If Alice is a friend of Bob, then Bob is also a friend of Alice. To incorporate this two-way direction relationship, we will need to merge the friendship table into itself.

Here's how the query looks:

SELECT user_id, friend_id
FROM friendship
UNION
SELECT friend_id, user_id
FROM friendship;
Showing the output of the first 5 rows:

user_id	friend_id
alice	david
charles	alice
alice	charles
david	alice
bob	alice
Step 2: Create a table containing information on users, their friends, and the pages recommended

We are recommending pages based on the user's friends, so we will use LEFT JOIN on the newly merged table with the page_following table. This will provide us with all the pages which we can recommend to the user.

Why are we not using an INNER JOIN? Well, we want all the combinations of the pages followed by each user's friend. In cases where the user's friend is not following any page, using an inner join would exclude the friend from the output.

Note: For this question, both LEFT JOIN and INNER JOIN would give you the same output due to the straightforward input of the data set. However, it's crucial for you to understand how the data is being merged as both joins are popular questions in technical interviews. Here's a graphical presentation on the differences between them.

The query in step 1 is converted into a CTE called two_way_friendship.

WITH two_way_friendship AS (
-- Insert the query in step 1)

SELECT
  friends.user_id,
  pages.page_id,
  COUNT(*) AS followers
FROM two_way_friendship AS friends
LEFT JOIN page_following AS pages
  ON friends.friend_id = pages.user_id
GROUP BY friends.user_id, pages.page_id;
user_id	page_id	followers
david	google	2
charles	facebook	1
alice	facebook	1
david	linkedin	1
david	facebook	2
David is being recommended to follow Google which 2 of his friends are following whereas Alice is recommended to follow Facebook as one of her friends is following Facebook.

Step 3: Exclude the pages that the users have been following

But, there's a catch! We don't want to recommend pages that the users already liked because that would be redundant. So, we need to check for their existence and exclude those pages. To do this, we can use NOT EXISTS operator as shown below.

Also, we are recommending for each user, the pages with the highest number of followers who are friends. In other word, we are counting friends for each (user_id, page_id). Be careful with what to put in the GROUP BY clause and what to put in the COUNT() function.

WITH two_way_friendship AS (
-- Insert the query in step 1)

SELECT
  friends.user_id,
  pages.page_id,
  COUNT(*) AS followers
FROM two_way_friendship AS friends
LEFT JOIN page_following AS pages
  ON friends.friend_id = pages.user_id
WHERE NOT EXISTS (
  SELECT id
  FROM page_following AS pages_2
  WHERE friends.user_id = pages_2.user_id
    AND pages.page_id = pages_2.page_id)
GROUP BY friends.user_id, pages.page_id;
This is what the output of this step will look like:

user_id	page_id	followers
david	google	2
charles	facebook	1
david	facebook	2
bob	github	1
alice	linkedin	3
The output no longer contains the recommendation of Facebook page for Alice. Does it mean that Alice is already following Facebook?

Let's check whether our theory is true.

SELECT *
FROM page_following
WHERE user_id = 'alice';
user_id	page_id
alice	google
alice	facebook
We're right to say that as Alice is already following the Facebook page. Our page recommender is working as it should!

Step 4:

In the final step, we will find the top page recommendation for each user by counting the number of followers for each page by users.

We can use the window function DENSE_RANK which generates a numbering system to rank pages for every user based on the total number of followers.

WITH two_way_friendship AS (
-- Insert query in step 1
), recommended_pages AS (
-- Insert query in step 3
)

SELECT
  user_id,
  page_id,
  followers,
  DENSE_RANK() OVER (
    PARTITION BY user_id ORDER BY followers DESC) AS rnk
FROM recommended_pages;
user_id	page_id	followers	rnk
bob	github	1	1
charles	facebook	1	1
david	google	2	1
david	facebook	2	1
Take a look at David's output. He has two friends who are following Google and Facebook pages. Since there is a tie between the number of friends following the pages, both pages receive the ranking of 1, hence Google and Facebook pages are the top recommended pages for David.

What happens when you use the other two window functions, ROW_NUMBER and RANK?

user_id	page_id	followers	row_number_rnk	rank_rnk
bob	github	1	1	1
charles	facebook	1	1	1
david	google	2	1	1
david	facebook	2	2	1
Let's use David as an example. ROW_NUMBER function (row_number_rnk) ranks the Facebook page as 2 even though the follower count is the same because this function ranks the rows in ascending order, regardless of the values that were selected.

RANK (rank_rnk) produces a similar ranking with DENSE_RANK however, if David were to have another recommended page ie. Github with 1 follower, then the row would be given a rank of 3, which is not a desirable output as it skips the ranking 2.

WITH two_way_friendship AS (
  SELECT user_id, friend_id
  FROM friendship
  UNION
  SELECT friend_id, user_id
  FROM friendship
  
), recommended_pages AS (
  SELECT
    friends.user_id,
    pages.page_id,
    COUNT(*) AS followers
  FROM two_way_friendship AS friends
  LEFT JOIN page_following AS pages
    ON friends.friend_id = pages.user_id
  WHERE NOT EXISTS (
    SELECT id
    FROM page_following AS pages_2
    WHERE friends.user_id = pages_2.user_id
      AND pages.page_id = pages_2.page_id)
  GROUP BY friends.user_id, pages.page_id
  
), top_pages AS (
  SELECT
    user_id,
    page_id,
    followers,
    DENSE_RANK() OVER (
      PARTITION BY user_id ORDER BY followers DESC) AS rnk
  FROM recommended_pages)

SELECT user_id, page_id
FROM top_pages
WHERE rnk = 1
ORDER BY user_id;
Results:

user_id	page_id
alice	linkedin
bob	github
charles	facebook
david	google
david	facebook
"""
