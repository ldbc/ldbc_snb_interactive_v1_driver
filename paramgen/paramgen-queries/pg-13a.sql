-- variant (a): guaranteed that no path exists
SELECT
    component1.PersonId AS 'person1Id',
    component2.PersonId AS 'person2Id',
    GREATEST(component1.creationDate, component2.creationDate) AS 'useFrom',
    useUntil AS 'useUntil'
FROM
    (
        SELECT PersonId, Component, creationDate
          FROM personKnowsPersonConnected, personNumFriends
         WHERE personNumFriends.id = PersonId
         ORDER BY md5(PersonId + 1)
         LIMIT 100
    ) component1,
    (
        SELECT PersonId, Component, creationDate
          FROM personKnowsPersonConnected, personNumFriends
         WHERE personNumFriends.id = PersonId
         ORDER BY md5(PersonId + 2)
         LIMIT 100
    ) component2,
    (
        SELECT epoch_ms(:date_limit_long) AS useUntil
    )
WHERE component1.Component != component2.Component
  AND GREATEST(component1.creationDate, component2.creationDate) < epoch_ms(:date_limit_long)
ORDER BY md5(concat(component1.PersonId, component2.PersonId))
LIMIT 500
