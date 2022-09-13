SELECT Person1Id AS personId, 
  FROM personNumFriendsOfFriendsOfFriends 
 WHERE numFriends > 0 AND deletionDate > '2019' AND creationDate < :date_limit_long
 ORDER BY md5(Person1Id) 
LIMIT 50  