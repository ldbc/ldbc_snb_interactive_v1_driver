SELECT
    personId AS 'personId',
    creationDay AS 'startDate',
    2 + salt * 37 % 5 AS 'durationDays',
       useFrom AS 'useFrom',
       useUntil AS 'useUntil'
FROM
    (
        SELECT Person1Id AS personId,
               creationDate AS useFrom,
               deletionDate AS useUntil
          FROM personNumFriendsSelected
         WHERE deletionDate - INTERVAL 1 DAY > :date_limit_filter
           AND creationDate + INTERVAL 1 DAY < :date_limit_filter
         ORDER BY diff, md5(Person1Id)
         LIMIT 10
    ),
    (
        SELECT creationDay
        FROM creationDayNumMessagesSelected
        ORDER BY md5(creationDay)
        LIMIT 10
    ),
    (SELECT unnest(generate_series(1, 20)) AS salt)
ORDER BY md5(concat(personId, creationDay, salt))
LIMIT 500