-- -- variant (b): guaranteed that there is a path
-- NOTE: This sql script is the same as the Q13b one. Therefore, in the paramgen.py, this one is skipped
-- and the result of Q13b is exported twice (interactive-13b.parquet & interactive-14b.parquet)
SELECT DISTINCT
    p1Id AS 'person1Id',
    p2Id AS 'person2Id',
    GREATEST(knows4.creationDay, knows3.creationDay, creationDayFirstHalf) AS 'useFrom',
    LEAST(knows4.deletionDay, knows3.deletionDay, deletionDayFirstHalf) AS 'useUntil'
FROM
  (
    SELECT DISTINCT
      people4Hops_sample.person1Id AS p1Id,
      people4Hops_sample.person2Id AS p2Id,
      knows2.person2Id AS middleCandidate,
      GREATEST(knows1.creationDay, knows2.creationDay) as creationDayFirstHalf,
      LEAST(knows1.deletionDay, knows2.deletionDay) as deletionDayFirstHalf
    FROM (
      SELECT *
      FROM people4Hops
      LIMIT 80
    ) people4Hops_sample
    -- two hops from person1Id
    JOIN personKnowsPersonDays knows1
      ON knows1.person1Id = people4Hops_sample.person1Id
    JOIN personKnowsPersonDays knows2
      ON knows2.person1Id = knows1.person2Id
  ) sub

-- two hops from person2Id
JOIN personKnowsPersonDays knows4
  ON knows4.person1Id = p2Id
JOIN personKnowsPersonDays knows3
  ON knows3.person1Id = knows4.person2Id

-- meet in the middle
WHERE middleCandidate = knows3.person2Id
  AND GREATEST(knows4.creationDay, knows3.creationDay, creationDayFirstHalf) + INTERVAL 1 DAY < :date_limit_filter
  AND LEAST(knows4.deletionDay, knows3.deletionDay, deletionDayFirstHalf) - INTERVAL 1 DAY > :date_limit_filter
ORDER BY md5(131*p1Id + 241*p2Id), md5(p1Id)
