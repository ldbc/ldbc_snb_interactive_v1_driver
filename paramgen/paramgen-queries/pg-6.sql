SELECT
    personId AS 'personId',
     tagName AS 'tagName',
     useFrom AS 'useFrom',
    useUntil AS 'useUntil'
FROM
    (
        SELECT Person1Id AS personId,
               creationDate AS useFrom,
               deletionDate AS useUntil
          FROM personNumFriendsOfFriendsSelected
         WHERE deletionDate - INTERVAL 1 DAY > :date_limit_filter
           AND creationDate + INTERVAL 1 DAY < :date_limit_filter
         ORDER BY diff, md5(Person1Id)
         LIMIT 50
    ),
    (
        SELECT name AS tagName,
               abs(frequency - (
                    SELECT percentile_disc(0.25)
                    WITHIN GROUP (ORDER BY frequency)
                    FROM tagNumPersons)
               ) AS diff
    FROM tagNumPersons
    ORDER BY diff, md5(name)
    LIMIT 15
    )
ORDER BY md5(concat(personId, tagName))
LIMIT 500
