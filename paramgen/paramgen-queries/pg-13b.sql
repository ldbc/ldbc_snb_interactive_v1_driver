-- variant (b): guaranteed that there is a path
SELECT person1Id AS 'person1Id',
       person2Id AS 'person2Id',
       useFrom,
       useUntil
  FROM
  (
        SELECT person1Id,
               person2Id,
               useFrom,
               useUntil
          FROM people4Hops
         WHERE useFrom + INTERVAL 1 DAY < :date_limit_filter
           AND useUntil - INTERVAL 1 DAY  > :date_limit_filter
  )
ORDER BY md5(concat(person1Id + 1, person2Id + 2)) ASC
LIMIT 500
