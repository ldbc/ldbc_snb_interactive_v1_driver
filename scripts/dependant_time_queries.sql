-- Deletes
COPY (SELECT deletionDate, creationDate as dependentDate, id
        FROM Comment
        WHERE deletionDate > :start_date_long
        AND explicitlyDeleted = true
        ORDER BY deletionDate ASC)
TO ':output_dir/deletes/Comment.parquet' (FORMAT 'parquet');


COPY (SELECT deletionDate, creationDate as dependentDate, id
        FROM Post
        WHERE deletionDate > :start_date_long
        AND explicitlyDeleted = true
        ORDER BY deletionDate ASC)
TO ':output_dir/deletes/Post.parquet' (FORMAT 'parquet');


COPY (
    SELECT  Person_likes_Post.deletionDate,
            GREATEST(Person.creationDate, Post.creationDate) AS dependentDate,
            Person_likes_Post.PersonId,
            Person_likes_Post.PostId
      FROM Person, Person_likes_Post, Post
     WHERE Person_likes_Post.deletionDate > :start_date_long
       AND Person_likes_Post.explicitlyDeleted = true
       AND Person_likes_Post.PersonId = Person.id
       AND Person_likes_Post.PostId = Post.id
     ORDER BY Person_likes_Post.deletionDate ASC
    )
TO ':output_dir/deletes/Person_likes_Post.parquet' (FORMAT 'parquet');


COPY (SELECT deletionDate, creationDate as dependentDate, id
        FROM Person
        WHERE deletionDate > :start_date_long
        AND explicitlyDeleted = true
        ORDER BY deletionDate ASC)
TO ':output_dir/deletes/Person.parquet' (FORMAT 'parquet');

COPY (SELECT deletionDate, creationDate as dependentDate, id
        FROM Forum
        WHERE deletionDate > :start_date_long
        AND explicitlyDeleted = true
        ORDER BY deletionDate ASC)
TO ':output_dir/deletes/Forum.parquet' (FORMAT 'parquet');

COPY (
    SELECT  Person_likes_Comment.deletionDate,
            GREATEST(Person.creationDate, Comment.creationDate) AS dependentDate,
            Person_likes_Comment.PersonId,
            Person_likes_Comment.CommentId
      FROM Person, Person_likes_Comment, Comment
     WHERE Person_likes_Comment.deletionDate > :start_date_long
       AND Person_likes_Comment.explicitlyDeleted = true
       AND Person_likes_Comment.PersonId = Person.id
       AND Person_likes_Comment.CommentId = Comment.id
     ORDER BY Person_likes_Comment.deletionDate ASC
    )
TO ':output_dir/deletes/Person_likes_Comment.parquet' (FORMAT 'parquet');


COPY (
    SELECT  Person_knows_Person.deletionDate,
            GREATEST(Person1.creationDate, Person2.creationDate) AS dependentDate,
            Person_knows_Person.Person1Id,
            Person_knows_Person.Person2Id
       FROM Person Person1, Person Person2, Person_knows_Person
      WHERE Person_knows_Person.deletionDate > :start_date_long
        AND Person_knows_Person.explicitlyDeleted = true
        AND Person_knows_Person.Person1Id = Person1.id
        AND Person_knows_Person.Person2Id = Person2.id
      ORDER BY Person_knows_Person.deletionDate ASC
)
TO ':output_dir/deletes/Person_knows_Person.parquet' (FORMAT 'parquet');

COPY (
    SELECT  Forum_hasMember_Person.deletionDate,
            GREATEST(Person.creationDate, Forum.creationDate) as dependentDate,
            Forum_hasMember_Person.ForumId,
            Forum_hasMember_Person.PersonId
       FROM Person, Forum_hasMember_Person, Forum
      WHERE Forum_hasMember_Person.deletionDate > :start_date_long
        AND Forum_hasMember_Person.explicitlyDeleted = true
        AND Forum_hasMember_Person.PersonId = Person.id
        AND Forum_hasMember_Person.ForumId = Forum.id
      ORDER BY Forum_hasMember_Person.deletionDate ASC
)
TO ':output_dir/deletes/Forum_hasMember_Person.parquet' (FORMAT 'parquet');

-- Inserts

COPY (
    SELECT
        Person.creationDate,
        0 AS dependencyTime,
        Person.id,
        Person.firstName,
        Person.lastName,
        Person.gender,
        Person.birthday,
        Person.locationIP,
        Person.browserUsed,
        Person.LocationCityId,
        Person.language,
        Person.email,
        string_agg(DISTINCT Person_hasInterest_Tag.:tag_column_name, ';') AS tagIds,
        string_agg(DISTINCT Person_studyAt_University.UniversityId || ',' || Person_studyAt_University.classYear, ';') AS studyAt,
        string_agg(DISTINCT Person_workAt_Company.CompanyId || ',' || Person_workAt_Company.workFrom, ';') AS workAt
    FROM Person
    LEFT JOIN Person_studyAt_University
           ON Person_studyAt_University.PersonId = Person.id
    LEFT JOIN Person_workAt_Company
           ON Person_workAt_Company.PersonId = Person.id
    LEFT JOIN Person_hasInterest_Tag
           ON Person_hasInterest_Tag.PersonId = Person.id
    WHERE Person.creationDate > :start_date_long
    GROUP BY ALL
    ORDER BY Person.creationDate
)
TO ':output_dir/inserts/Person.parquet' (FORMAT 'parquet');

COPY (
    SELECT
        Forum.creationDate,
        Person.creationDate as dependencyTime,
        Forum.id,
        Forum.title,
        Forum.ModeratorPersonId,
        string_agg(DISTINCT Forum_hasTag_Tag.TagId, ';') AS tagIds
    FROM Person, Forum
    LEFT JOIN Forum_hasTag_Tag
           ON Forum_hasTag_Tag.ForumId = Forum.id
    WHERE Forum.creationDate > :start_date_long
      AND Forum.ModeratorPersonId = Person.id
    GROUP BY ALL
    ORDER BY Forum.creationDate
)
TO ':output_dir/inserts/Forum.parquet' (FORMAT 'parquet');


COPY (
    SELECT Person_knows_Person.creationDate,
           GREATEST(Person1.creationDate, Person2.creationDate) AS dependentDate,
      FROM Person Person1, Person Person2, Person_knows_Person
     WHERE Person_knows_Person.creationDate > :start_date_long
       AND Person_knows_Person.Person1Id = Person1.id
       AND Person_knows_Person.Person2Id = Person2.id
    ORDER BY Person_knows_Person.creationDate
)
TO ':output_dir/inserts/Person_knows_Person.parquet' (FORMAT 'parquet');

COPY (
    SELECT  Person_likes_Post.creationDate,
            GREATEST(Person.creationDate, Post.creationDate) AS dependentDate,
            Person_likes_Post.PersonId,
            Person_likes_Post.PostId
      FROM Person, Person_likes_Post, Post
     WHERE Person_likes_Post.creationDate > :start_date_long
       AND Person_likes_Post.PersonId = Person.id
       AND Person_likes_Post.PostId = Post.id
     ORDER BY Person_likes_Post.creationDate ASC
    )
TO ':output_dir/inserts/Person_likes_Post.parquet' (FORMAT 'parquet');

COPY (
    SELECT  Person_likes_Comment.creationDate,
            GREATEST(Person.creationDate, Comment.creationDate) AS dependentDate,
            Person_likes_Comment.PersonId,
            Person_likes_Comment.CommentId
      FROM Person, Person_likes_Comment, Comment
     WHERE Person_likes_Comment.creationDate > :start_date_long
       AND Person_likes_Comment.PersonId = Person.id
       AND Person_likes_Comment.CommentId = Comment.id
     ORDER BY Person_likes_Comment.creationDate ASC
    )
TO ':output_dir/inserts/Person_likes_Comment.parquet' (FORMAT 'parquet');

COPY (
    SELECT  Forum_hasMember_Person.creationDate,
            GREATEST(Person.creationDate, Forum.creationDate) AS dependentDate,
            Forum_hasMember_Person.PersonId,
            Forum_hasMember_Person.ForumId
      FROM Person, Forum_hasMember_Person, Forum
     WHERE Forum_hasMember_Person.creationDate > :start_date_long
       AND Forum_hasMember_Person.PersonId = Person.id
       AND Forum_hasMember_Person.ForumId = Forum.id
     ORDER BY Forum_hasMember_Person.creationDate ASC
    )
TO ':output_dir/inserts/Forum_hasMember_Person.parquet' (FORMAT 'parquet');
