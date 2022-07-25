---- drop tables

-- processed inserts
DROP TABLE IF EXISTS Comment_Insert;
DROP TABLE IF EXISTS Forum_Insert;
DROP TABLE IF EXISTS Forum_hasMember_Person_Insert;
DROP TABLE IF EXISTS Person_Insert;
DROP TABLE IF EXISTS Person_knows_Person_Insert;
DROP TABLE IF EXISTS Person_likes_Comment_Insert;
DROP TABLE IF EXISTS Person_likes_Post_Insert;
DROP TABLE IF EXISTS Post_Insert;

-- deletes
DROP TABLE IF EXISTS Comment_Delete;
DROP TABLE IF EXISTS Forum_Delete;
DROP TABLE IF EXISTS Person_Delete;
DROP TABLE IF EXISTS Post_Delete;
DROP TABLE IF EXISTS Forum_hasMember_Person_Delete;
DROP TABLE IF EXISTS Person_knows_Person_Delete;
DROP TABLE IF EXISTS Person_likes_Comment_Delete;
DROP TABLE IF EXISTS Person_likes_Post_Delete;

---- create tables
---- processed inserts
-- INS 7
CREATE TABLE Comment_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint PRIMARY KEY,
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
CREATE TABLE Forum_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint PRIMARY KEY,
    title varchar(256) NOT NULL,
    ModeratorPersonId bigint,
    TagIds string
);

-- INS 6
CREATE TABLE Post_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint PRIMARY KEY,
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
CREATE TABLE Person_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint PRIMARY KEY,
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
CREATE TABLE Forum_hasMember_Person_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    ForumId bigint NOT NULL
);

-- INS 3
CREATE TABLE Person_likes_Comment_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

-- INS 2
CREATE TABLE Person_likes_Post_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

-- INS 8
CREATE TABLE Person_knows_Person_Insert (
    creationDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);

---- deletes

-- DEL 7
CREATE TABLE Comment_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint PRIMARY KEY
);

-- DEL 4
CREATE TABLE Forum_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint PRIMARY KEY,
);

-- DEL 6
CREATE TABLE Post_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint PRIMARY KEY
);

-- DEL 1
CREATE TABLE Person_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    id bigint PRIMARY KEY
);

-- DEL 5
CREATE TABLE Forum_hasMember_Person_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

-- DEL 3
CREATE TABLE Person_likes_Comment_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

-- DEL 2
CREATE TABLE Person_likes_Post_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

-- DEL 8
CREATE TABLE Person_knows_Person_Delete (
    deletionDate bigint NOT NULL,
    dependentDate bigint NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);
