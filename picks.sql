

SELECT 
    a.radiant_team_name as rad, a.dire_team_name as dire, b.team_name as team, c.localized_name --, count(*) as cnt
FROM [match] a
INNER JOIN pick_ban b 
    ON a.match_id = b.match_id 
    AND b.is_pick = 'False'
LEFT JOIN hero_ref c 
    ON c.id = b.hero_id
where rad in ('Spider Pigzs', 'No Bounty Hunter')
and dire in ('Spider Pigzs', 'No Bounty Hunter')
group by team, c.localized_name
--order by team,  cnt desc