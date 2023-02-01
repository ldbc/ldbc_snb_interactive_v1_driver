SELECT personId  AS 'personId',
       firstName AS 'firstName',
       useFrom   AS 'useFrom',
       useUntil  AS 'useUntil'
FROM
    (
        SELECT Person1Id AS personId,
               creationDate AS useFrom,
               deletionDate AS useUntil
          FROM personNumFriendsOfFriendsOfFriendsSelected
         WHERE deletionDate - INTERVAL 1 DAY > :date_limit_filter
           AND creationDate + INTERVAL 1 DAY < :date_limit_filter
         ORDER BY md5(Person1Id)
         LIMIT 50
    ),
    (
        SELECT firstName AS firstName,
          FROM personFirstNamesSelected
         ORDER BY md5(firstName)
         LIMIT 20
    )
ORDER BY md5(concat(personId, firstName, useUntil, useFrom))
LIMIT 500
