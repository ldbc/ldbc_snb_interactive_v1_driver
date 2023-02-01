-- variant (b): guaranteed that there is a path
SELECT
    paths_and_posts.person1Id AS 'person1Id',
    paths_and_posts.person2Id AS 'person2Id',
    useFrom,
    useUntil
FROM
(
    SELECT
        people4Hops.person1Id,
        people4Hops.person2Id,
        people4Hops.useFrom,
        people4Hops.useUntil,
        abs(numFriendOfFriendPosts - (SELECT percentile_disc(0.05) WITHIN GROUP (ORDER BY numFriendOfFriendPosts) FROM personNumFriendOfFriendPosts)) AS diff,
    FROM people4Hops,
         personNumFriendOfFriendPosts posts
    WHERE people4Hops.person2Id = posts.person1Id 
      AND people4Hops.useFrom <= :date_limit_filter
      AND people4Hops.useUntil >= :date_limit_filter
      AND posts.deletionDate - INTERVAL 1 DAY  > :date_limit_filter
      AND posts.creationDate + INTERVAL 1 DAY < :date_limit_filter
      AND posts.numFriendOfFriendPosts > 0
      ORDER BY diff
      LIMIT 500
) paths_and_posts
ORDER BY md5(concat(paths_and_posts.person1Id + 3, paths_and_posts.person2Id + 4)), useFrom, useUntil ASC
LIMIT 500
