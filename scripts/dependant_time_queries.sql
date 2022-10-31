-- Deletes
COPY (SELECT deletionDate, creationDate as dependentDate, id
        FROM Comment
        WHERE deletionDate > :start_date_long
        AND deletionDate < :end_date_long
        AND explicitlyDeleted = true
        ORDER BY deletionDate ASC)
TO ':output_dir/Comment.parquet' (FORMAT 'parquet')

COPY (SELECT deletionDate, creationDate as dependentDate, id
        FROM Post
        WHERE deletionDate > :start_date_long
        AND deletionDate < :end_date_long
        AND explicitlyDeleted = true
        ORDER BY deletionDate ASC)
TO ':output_dir/Post.parquet' (FORMAT 'parquet')

COPY (SELECT deletionDate, creationDate as dependentDate, id
        FROM Person
        WHERE deletionDate > :start_date_long
        AND deletionDate < :end_date_long
        AND explicitlyDeleted = true
        ORDER BY deletionDate ASC)
TO ':output_dir/Person.parquet' (FORMAT 'parquet')

COPY (SELECT deletionDate, creationDate as dependentDate, id
        FROM Forum
        WHERE deletionDate > :start_date_long
        AND deletionDate < :end_date_long
        AND explicitlyDeleted = true
        ORDER BY deletionDate ASC)
TO ':output_dir/Forum.parquet' (FORMAT 'parquet')

COPY (
    SELECT  Person_likes_Comment.deletionDate,
            GREATEST(Person.creationDate, Comment.creationDate) AS dependentDate,
            Person_likes_Comment.PersonId,
            Person_likes_Comment.CommentId
      FROM Person, Person_likes_Comment, Comment
     WHERE Person_likes_Comment.deletionDate > :start_date_long
       AND Person_likes_Comment.deletionDate < :end_date_long
       AND Person_likes_Comment.explicitlyDeleted = true
       AND Person_likes_Comment.PersonId = Person.id
       AND Person_likes_Comment.CommentId = Comment.id
     ORDER BY Person_likes_Comment.deletionDate ASC
    )
TO ':output_dir/Person_likes_Comment.parquet' (FORMAT 'parquet');

COPY (
    SELECT  Person_likes_Post.deletionDate,
            GREATEST(Person.creationDate, Post.creationDate) AS dependentDate,
            Person_likes_Post.PersonId,
            Person_likes_Post.PostId
      FROM Person, Person_likes_Post, Post
     WHERE Person_likes_Post.deletionDate > :start_date_long
       AND Person_likes_Post.deletionDate < :end_date_long
       AND Person_likes_Post.explicitlyDeleted = true
       AND Person_likes_Post.PersonId = Person.id
       AND Person_likes_Post.PostId = Post.id
     ORDER BY Person_likes_Post.deletionDate ASC
    )
TO ':output_dir/Person_likes_Post.parquet' (FORMAT 'parquet');

COPY (
    SELECT  Person_knows_Person.deletionDate,
            GREATEST(Person1.creationDate, Person2.creationDate) AS dependentDate,
            Person_knows_Person.Person1Id,
            Person_knows_Person.Person2Id
       FROM Person Person1, Person Person2, Person_knows_Person
      WHERE Person_knows_Person.deletionDate > :start_date_long
        AND Person_knows_Person.deletionDate < :end_date_long
        AND Person_knows_Person.explicitlyDeleted = true
        AND Person_knows_Person.Person1Id = Person1.id
        AND Person_knows_Person.Person2Id = Person2.id
      ORDER BY Person_knows_Person.deletionDate ASC
)
TO ':output_dir/Person_knows_Person.parquet' (FORMAT 'parquet');

COPY (
    SELECT  Forum_hasMember_Person.deletionDate,
            GREATEST(Person.creationDate, Forum.creationDate) as dependentDate,
            Forum_hasMember_Person.ForumId,
            Forum_hasMember_Person.PersonId
       FROM Person, Forum_hasMember_Person, Forum
      WHERE Forum_hasMember_Person.deletionDate > {start_date_long}
        AND Forum_hasMember_Person.deletionDate < {end_date_long}
        AND Forum_hasMember_Person.explicitlyDeleted = true
        AND Forum_hasMember_Person.PersonId = Person.id
        AND Forum_hasMember_Person.ForumId = Forum.id
      ORDER BY Forum_hasMember_Person.deletionDate ASC
)
TO ':output_dir/Forum_hasMember_Person.parquet' (FORMAT 'parquet');

-- Inserts

