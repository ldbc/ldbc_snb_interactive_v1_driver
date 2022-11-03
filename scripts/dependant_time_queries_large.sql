-- inserts

COPY (
    SELECT  Forum_hasMember_Person.creationDate,
            GREATEST(Person.creationDate, Forum.creationDate) as dependentDate,
            Forum_hasMember_Person.ForumId,
            Forum_hasMember_Person.PersonId
       FROM Person, Forum, Forum_hasMember_Person
      WHERE Forum_hasMember_Person.creationDate > :start_date_long
        AND Forum_hasMember_Person.creationDate < :end_date_long
        AND Forum_hasMember_Person.explicitlyDeleted = true
        AND Forum_hasMember_Person.PersonId = Person.id
        AND Forum_hasMember_Person.ForumId = Forum.id
      ORDER BY Forum_hasMember_Person.creationDate ASC
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
    FROM Comment c2, Person, Comment c1
    LEFT JOIN Comment_hasTag_Tag
           ON Comment_hasTag_Tag.CommentId = c1.id
    WHERE c1.creationDate > :start_date_long
      AND c1.creationDate < :end_date_long
      AND Comment_hasTag_Tag.creationDate > :start_date_long
      AND Comment_hasTag_Tag.creationDate < :end_date_long
      AND c1.ParentPostId IS NULL AND c2.id = c1.ParentCommentId
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
    FROM Post, Person, Comment c1
    LEFT JOIN Comment_hasTag_Tag
           ON Comment_hasTag_Tag.CommentId = c1.id
    WHERE c1.creationDate > :start_date_long
      AND c1.creationDate < :end_date_long
      AND Comment_hasTag_Tag.creationDate > :start_date_long
      AND Comment_hasTag_Tag.creationDate < :end_date_long
      AND c1.ParentPostId = Post.id AND c1.ParentCommentId IS NULL
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
    FROM Forum, Person, Post po
    LEFT JOIN Post_hasTag_Tag
           ON Post_hasTag_Tag.PostId = po.id
    WHERE po.creationDate > :start_date_long
      AND po.creationDate < :end_date_long
      AND Post_hasTag_Tag.creationDate > :start_date_long
      AND Post_hasTag_Tag.creationDate < :end_date_long
      AND po.ContainerForumId = Forum.id
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
      FROM Person, Comment, Person_likes_Comment pc
     WHERE Comment.creationDate > :start_date_long
       AND Comment.creationDate < :end_date_long
       AND pc.creationDate > :start_date_long
       AND pc.creationDate < :end_date_long
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
      FROM Person, Post, Person_likes_Post pp
     WHERE pp.creationDate > :start_date_long
       AND pp.creationDate < :end_date_long
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
       FROM Person, Forum, Forum_hasMember_Person fp
      WHERE fp.deletionDate > :start_date_long
        AND fp.deletionDate < :end_date_long
        AND fp.explicitlyDeleted = true
        AND fp.PersonId = Person.id
        AND fp.ForumId = Forum.id
      ORDER BY fp.deletionDate ASC
)
TO ':output_dir/deletes/Forum_hasMember_Person-:index.parquet' (FORMAT 'parquet');
