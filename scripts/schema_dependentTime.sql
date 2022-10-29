---- processed inserts
-- INS 7
CREATE OR REPLACE TABLE Comment_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    content varchar(2000) NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint,
    TagIds string
);

-- INS 4
CREATE OR REPLACE TABLE Forum_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint,
    title varchar(256) NOT NULL,
    ModeratorPersonId bigint,
    TagIds string
);

-- INS 6
CREATE OR REPLACE TABLE Post_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint,
    imageFile varchar(40),
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    language varchar(40),
    content varchar(2000),
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    TagIds string
);

-- INS 1
CREATE OR REPLACE TABLE Person_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint,
    firstName varchar(40) NOT NULL,
    lastName varchar(40) NOT NULL,
    gender varchar(40) NOT NULL,
    birthday date NOT NULL,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks varchar(640) NOT NULL,
    email varchar(8192) NOT NULL,
    tagIds string,
    studyAt string,
    workAt string
);

-- INS 5
CREATE OR REPLACE TABLE Forum_hasMember_Person_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    ForumId bigint NOT NULL
);

-- INS 3
CREATE OR REPLACE TABLE Person_likes_Comment_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

-- INS 2
CREATE OR REPLACE TABLE Person_likes_Post_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

-- INS 8
CREATE OR REPLACE TABLE Person_knows_Person_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);

---- deletes

-- DEL 7
CREATE OR REPLACE TABLE Comment_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint
);

-- DEL 4
CREATE OR REPLACE TABLE Forum_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint,
);

-- DEL 6
CREATE OR REPLACE TABLE Post_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint
);

-- DEL 1
CREATE OR REPLACE TABLE Person_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint
);

-- DEL 5
CREATE OR REPLACE TABLE Forum_hasMember_Person_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

-- DEL 3
CREATE OR REPLACE TABLE Person_likes_Comment_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

-- DEL 2
CREATE OR REPLACE TABLE Person_likes_Post_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

-- DEL 8
CREATE OR REPLACE TABLE Person_knows_Person_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);
