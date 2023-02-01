-- Select the parameters with similar number of friends based on threshold.
-- In case there are multiple groups fitting the criteria, select the one
-- select the one with the lowest standard deviation
-- In this query, the threshold is set to 5 for difference when to mark another group
-- 3000 for the minimum number of friends and the group must at least contain 100 personIds

/*
params example:
    threshold: 5
    min_occurence: 100
    min_param_value: 3000
    window_column: numFriendsOfFriends
    param_column: Person1Id
    source_table : personNumFriendsOfFriendsOfFriends
    table_name  : personNumFriendsSelected
*/

WITH grouped_parameters AS (
    SELECT
        *, SUM(CASE WHEN Groups.diff < :threshold THEN 0 ELSE 1 END) OVER (ORDER BY Groups.RN) AS groupId
    FROM
    (
        SELECT ROW_NUMBER() OVER(ORDER BY :window_column) AS RN,
               :param_column, 
               :window_column,
               creationDate,
               deletionDate,
               abs(LAG(:window_column, 1)
                   OVER (ORDER BY :window_column ASC) - :window_column
               ) AS diff
          FROM :source_table
         WHERE :window_column > :min_param_value
    ) Groups
),

cte AS (
    SELECT groupId, occurence, deviation
      FROM (
            SELECT groupId, count(groupId) AS occurence, stddev_pop(:window_column) as deviation
            FROM grouped_parameters
            GROUP BY groupId
      ) group_stats
      WHERE group_stats.occurence > :min_occurence
      ORDER BY deviation ASC
      LIMIT 1
)

SELECT *
  FROM grouped_parameters, cte
 WHERE grouped_parameters.groupId = (SELECT groupId FROM cte)
