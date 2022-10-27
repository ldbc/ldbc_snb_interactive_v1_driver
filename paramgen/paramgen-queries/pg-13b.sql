-- variant (b): guaranteed that there is a path
SELECT
    person1Id AS 'person1Id',
    person2Id AS 'person2Id',
    useFrom,
    useUntil
FROM people4Hops,
(
        SELECT Person1Id AS personId,
               numFriendsOfFriends,
               abs(numFriendsOfFriends - (
                    SELECT percentile_disc(0.65)
                    WITHIN GROUP (ORDER BY numFriendsOfFriends)
                      FROM personNumFriendsOfFriendsOfFriends)
               ) AS diff
          FROM personNumFriendsOfFriendsOfFriends
         WHERE numFriends > 0
         ORDER BY diff, md5(Person1Id)
         LIMIT 100
    ) personIds
WHERE people4Hops.Person1Id = personIds.personId
ORDER BY md5(concat(person1Id + 1, person2Id + 2))
LIMIT 500