SELECT
    personId AS 'personId',
    1 + salt * 37 % 12 AS 'month',
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
         LIMIT 50
    ),
    (
        SELECT unnest(generate_series(1, 20)) AS salt
    )
ORDER BY md5(concat(personId, salt))
LIMIT 500
