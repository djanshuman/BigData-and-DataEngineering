'''
https://platform.stratascratch.com/coding/2059-player-with-longest-streak?code_type=1
You are given a table of tennis players and their matches that they could either win (W) or lose (L). Find the longest streak of wins. A streak is a set of consecutive won matches of one player. 
  
The streak ends once a player loses their next match. Output the ID of the player or players and the length of the streak.

Expected Output

player_id	streak_length
402	5
403	5
  
players_results

player_id	match_date	match_result
401	2021-05-04	W
401	2021-05-09	L
401	2021-05-16	L
401	2021-05-18	W
401	2021-05-22	L
'''
WITH match_ordered AS
  (SELECT player_id,
          match_date,
          match_result,
          LAG(match_result) OVER (PARTITION BY player_id
                                  ORDER BY match_date) AS prev_result
   FROM players_results),
   
     streak_flags AS
  (SELECT player_id,
          match_date,
          match_result,
          CASE
              WHEN match_result = prev_result THEN 0
              ELSE 1
          END AS streak_change
   FROM match_ordered),
   
     streak_groups AS
  (SELECT player_id,
          match_date,
          match_result,
          SUM(streak_change) OVER (PARTITION BY player_id
                                   ORDER BY match_date ROWS UNBOUNDED PRECEDING) AS streak_id
   FROM streak_flags),
   
     win_streaks AS
  (SELECT player_id,
          streak_id,
          COUNT(*) AS streak_length
   FROM streak_groups
   WHERE match_result = 'W'
   GROUP BY player_id,
            streak_id),
            
     ranked_streaks AS
  (SELECT player_id,
          streak_length,
          RANK() OVER (
                       ORDER BY streak_length DESC) AS rnk
   FROM win_streaks)
   
   
SELECT player_id,
       streak_length
FROM ranked_streaks
WHERE rnk = 1

'''
Output

Your Solution:
player_id	streak_length
402	5
403	5
'''
