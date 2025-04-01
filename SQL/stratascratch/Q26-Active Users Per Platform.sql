/*

Active Users Per Platform
https://platform.stratascratch.com/coding/2072-active-users-per-platform?code_type=1

For each platform (e.g. Windows, iPhone, iPad etc.), calculate the number of users. Consider unique users and not individual sessions. Output the name of the platform with the corresponding number of users.

*/


select platform,
        count(distinct user_id) as n_users
from user_sessions
group by platform;
