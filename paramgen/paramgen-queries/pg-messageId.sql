SELECT MessageId AS MessageId, 
  FROM messageIds 
 WHERE deletionDay > :date_limit_filter AND creationDay < :date_limit_filter
 ORDER BY md5(MessageId) 
LIMIT 50  