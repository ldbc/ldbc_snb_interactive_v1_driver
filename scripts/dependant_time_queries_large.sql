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
