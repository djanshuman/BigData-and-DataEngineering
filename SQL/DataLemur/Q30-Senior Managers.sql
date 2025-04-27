"""
https://datalemur.com/questions/senior-managers-reportees
Assume we have a table of Google employees with their corresponding managers.

A manager is an employee with a direct report. A senior manager is an employee who manages at least one manager, but none of their direct reports is senior managers themselves. Write a query to find the senior managers and their direct reports.

Output the senior manager's name and the count of their direct reports. The senior manager with the most direct reports should be the first result.

Assumption:

An employee can report to two senior managers.
employees Table:
Column Name	Type
emp_id	integer
manager_id	integer
manager_name	string
  
employees Example Input:
emp_id	manager_id	manager_name
1	101	Duyen
101	1001	Rick
103	1001	Rick
1001	1008	John
  
Example Output:
manager_name	direct_reportees
Rick	1
Rick is a senior manager who has one manager directly reporting to him, which is employee id 101.

The dataset you are querying against may have different input & output - this is just an example!
  
"""
# we need to find the manager name and count , so we choose manager table.
# senior_manager table has senior manager details, the question asks about manager details.
SELECT
  manager.manager_name as "manager_name",
  count(distinct manager.emp_id) as "direct_reportees"
    from employees -- Represent employees
    inner join employees manager -- Represent managers
      on employees.manager_id = manager.emp_id
    inner join employees senior_manager       -- Represent senior managers
      on manager.manager_id = senior_manager.emp_id
  group by manager.manager_name
  order by count(manager.emp_id) desc;

"""
Output

manager_name	direct_reportees
Rick	2
Tom	1
"""
