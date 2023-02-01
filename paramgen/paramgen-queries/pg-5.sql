SELECT
    personId AS 'personId',
    creationDay AS 'minDate',
    useFrom,
    useUntil
FROM
    (
        SELECT Person1Id AS personId,
               numFriendOfFriendForums,
               abs(numFriendOfFriendForums - (
                    SELECT percentile_disc(0.005)
                    WITHIN GROUP (ORDER BY numFriendOfFriendForums)
                      FROM personNumFriendOfFriendForums)
               ) AS diff,
               creationDate AS useFrom,
               deletionDate AS useUntil
          FROM personNumFriendOfFriendForums
         WHERE numFriendOfFriendForums > 0
           AND deletionDate - INTERVAL 1 DAY > :date_limit_filter
           AND creationDate + INTERVAL 1 DAY < :date_limit_filter
         ORDER BY diff, md5(Person1Id)
         LIMIT 50
    ),
    (
        SELECT creationDay
        FROM creationDayNumMessagesSelected
        ORDER BY md5(creationDay)
        LIMIT 10
    ),
ORDER BY useFrom, useUntil, md5(concat(personId, creationDay))
LIMIT 500
