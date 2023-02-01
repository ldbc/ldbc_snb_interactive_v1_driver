SELECT
    personId AS 'personId',
    creationDay AS 'maxDate',
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
         ORDER BY md5(Person1Id)
         LIMIT 50
    ),
    (
        SELECT creationDay
        FROM creationDayNumMessagesSelected
        ORDER BY md5(creationDay)
        LIMIT 20
    )
ORDER BY md5(concat(personId, creationDay))
LIMIT 500
