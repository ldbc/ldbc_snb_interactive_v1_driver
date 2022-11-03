-- inserts

COPY (
    SELECT  fp.creationDate,
            GREATEST(Person.creationDate, Forum.creationDate) as dependentDate,
            fp.ForumId,
            fp.PersonId
       FROM Person, Forum, (
        SELECT *
          FROM Forum_hasMember_Person
         WHERE Forum_hasMember_Person.creationDate > :start_date_long
           AND Forum_hasMember_Person.creationDate < :end_date_long
    ) fp
      WHERE fp.explicitlyDeleted = true
        AND fp.PersonId = Person.id
        AND fp.ForumId = Forum.id
      ORDER BY fp.creationDate ASC
)
TO ':output_dir/inserts/Forum_hasMember_Person-:index.parquet' (FORMAT 'parquet');

COPY (
    SELECT
        c1.creationDate,
        GREATEST(Person.creationDate, c2.creationDate) AS dependencyTime,
        c1.id,
        c1.locationIP,
        c1.browserUsed,
        c1.content,
        c1.length,
        c1.CreatorPersonId,
        c1.LocationCountryId,
        c1.ParentPostId,
        c1.ParentCommentId,
        string_agg(DISTINCT Comment_hasTag_Tag.TagId, ';') AS tagIds
    FROM Comment c2, Person, (
        SELECT *
          FROM Comment
         WHERE Comment.creationDate > :start_date_long
           AND Comment.creationDate < :end_date_long
    ) c1
    LEFT JOIN Comment_hasTag_Tag
           ON Comment_hasTag_Tag.CommentId = c1.id
    WHERE c1.ParentPostId IS NULL AND c2.id = c1.ParentCommentId
      AND c1.CreatorPersonId = Person.id
    GROUP BY ALL
    ORDER BY c1.creationDate
)
TO ':output_dir/inserts/Comment-:index.parquet' (FORMAT 'parquet');

COPY (
    SELECT
        c1.creationDate,
        GREATEST(Person.creationDate, Post.creationDate) AS dependencyTime,
        c1.id,
        c1.locationIP,
        c1.browserUsed,
        c1.content,
        c1.length,
        c1.CreatorPersonId,
        c1.LocationCountryId,
        c1.ParentPostId,
        c1.ParentCommentId,
        string_agg(DISTINCT Comment_hasTag_Tag.TagId, ';') AS tagIds
    FROM Post, Person, (
        SELECT *
          FROM Comment
         WHERE Comment.creationDate > :start_date_long
           AND Comment.creationDate < :end_date_long
    ) c1
    LEFT JOIN Comment_hasTag_Tag
           ON Comment_hasTag_Tag.CommentId = c1.id
    WHERE c1.ParentPostId = Post.id AND c1.ParentCommentId IS NULL
      AND c1.CreatorPersonId = Person.id
    GROUP BY ALL
    ORDER BY c1.creationDate
)
TO ':output_dir/inserts/Comment-:index.parquet' (FORMAT 'parquet');


COPY (
    SELECT
        po.creationDate,
        GREATEST(Person.creationDate, Forum.creationDate) AS dependencyTime,
        po.id,
        po.imageFile,
        po.locationIP,
        po.browserUsed,
        po.language,
        po.content,
        po.length,
        po.CreatorPersonId,
        po.ContainerForumId,
        po.LocationCountryId,
        string_agg(DISTINCT Post_hasTag_Tag.TagId, ';') AS tagIds
    FROM Forum, Person, (
        SELECT *
          FROM Post
         WHERE Post.creationDate > :start_date_long
           AND Post.creationDate < :end_date_long
    ) po
    LEFT JOIN Post_hasTag_Tag
           ON Post_hasTag_Tag.PostId = po.id
    WHERE po.ContainerForumId = Forum.id
      AND po.CreatorPersonId = Person.id
    GROUP BY ALL
    ORDER BY po.creationDate
)
TO ':output_dir/inserts/Post-:index.parquet' (FORMAT 'parquet');

COPY (
    SELECT  pc.creationDate,
            GREATEST(Person.creationDate, Comment.creationDate) AS dependentDate,
            pc.PersonId,
            pc.CommentId
      FROM Person, Comment, (
              SELECT *
                FROM Person_likes_Comment
              WHERE Person_likes_Comment.creationDate > :start_date_long
                AND Person_likes_Comment.creationDate < :end_date_long
      ) pc
     WHERE pc.creationDate > :start_date_long
       AND pc.PersonId = Person.id
       AND pc.CommentId = Comment.id
     ORDER BY pc.creationDate ASC
    )
TO ':output_dir/inserts/Person_likes_Comment-:index.parquet' (FORMAT 'parquet');

COPY (
    SELECT  pp.creationDate,
            GREATEST(Person.creationDate, Post.creationDate) AS dependentDate,
            pp.PersonId,
            pp.PostId
      FROM Person, Post, (
              SELECT *
                FROM Person_likes_Post
              WHERE Person_likes_Post.creationDate > :start_date_long
                AND Person_likes_Post.creationDate < :end_date_long
      ) pp
     WHERE pp.creationDate > :start_date_long
       AND pp.PersonId = Person.id
       AND pp.PostId = Post.id
     ORDER BY pp.creationDate ASC
    )
TO ':output_dir/inserts/Person_likes_Post-:index.parquet' (FORMAT 'parquet');

-- deletes

COPY (
    SELECT  fp.deletionDate,
            GREATEST(Person.creationDate, Forum.creationDate) as dependentDate,
            fp.ForumId,
            fp.PersonId
       FROM Person, Forum, (
        SELECT *
          FROM Forum_hasMember_Person
         WHERE Forum_hasMember_Person.deletionDate > :start_date_long
           AND Forum_hasMember_Person.deletionDate < :end_date_long
    ) fp
      WHERE fp.explicitlyDeleted = true
        AND fp.PersonId = Person.id
        AND fp.ForumId = Forum.id
      ORDER BY fp.deletionDate ASC
)
TO ':output_dir/deletes/Forum_hasMember_Person-:index.parquet' (FORMAT 'parquet');
