SELECT
    personId AS 'personId',
    useFrom  AS 'useFrom',
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
    )
ORDER BY md5(personId)
LIMIT 500
