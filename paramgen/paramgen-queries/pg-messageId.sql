SELECT MessageId AS MessageId, 
  FROM messageIds 
 WHERE deletionDate > :date_limit_long AND creationDate < :date_limit_long
 ORDER BY md5(MessageId) 
LIMIT 50  