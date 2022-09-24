-- variant (b): guaranteed that there is a path
SELECT
    person1Id AS 'person1Id',
    person2Id AS 'person2Id',
    GREATEST(Person1CreationDate, Person2CreationDate) AS 'useFrom',
       LEAST(Person1DeletionDate, Person2DeletionDate) AS 'useUntil'
FROM people4Hops,
(
        SELECT Person1Id AS personId,
               numFriendsOfFriends,
               abs(numFriendsOfFriends - (
                    SELECT percentile_disc(0.65)
                    WITHIN GROUP (ORDER BY numFriendsOfFriends)
                      FROM personNumFriendsOfFriendsOfFriends)
               ) AS diff,
               creationDate AS useFrom,
               deletionDate AS useUntil
          FROM personNumFriendsOfFriendsOfFriends
         WHERE numFriends > 0 AND deletionDate - INTERVAL 1 DAY  > :date_limit_filter AND creationDate + INTERVAL 1 DAY < :date_limit_filter
         ORDER BY diff, md5(Person1Id)
         LIMIT 100
    ) personIds
WHERE Person1CreationDate + INTERVAL 1 DAY < :date_limit_filter
  AND Person2CreationDate + INTERVAL 1 DAY < :date_limit_filter
  AND Person1DeletionDate - INTERVAL 1 DAY > :date_limit_filter
  AND Person2DeletionDate - INTERVAL 1 DAY > :date_limit_filter
  AND people4Hops.Person1Id = personIds.personId
ORDER BY md5(concat(person1Id + 1, person2Id + 2))
LIMIT 500
