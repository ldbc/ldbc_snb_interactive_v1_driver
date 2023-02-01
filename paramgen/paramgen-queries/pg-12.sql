SELECT
    personId AS 'personId',
    tagClassName AS 'tagClassName',
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
         LIMIT 50
    ),
    (
        SELECT name AS tagClassName,
               frequency AS freq,
               abs(frequency - (
                   SELECT percentile_disc(0.1)
                   WITHIN GROUP (ORDER BY frequency)
                   FROM tagClassNumTags)
               ) AS diff
          FROM tagClassNumTags
         ORDER BY diff, tagClassName
         LIMIT 50
    )
ORDER BY md5(concat(personId, tagClassName))
LIMIT 500
