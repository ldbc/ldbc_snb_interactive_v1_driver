---- drop tables

-- vanilla inserts
DROP TABLE IF EXISTS Comment;
DROP TABLE IF EXISTS Comment_hasTag_Tag;
DROP TABLE IF EXISTS Forum;
DROP TABLE IF EXISTS Forum_hasMember_Person;
DROP TABLE IF EXISTS Forum_hasTag_Tag;
DROP TABLE IF EXISTS Person;
DROP TABLE IF EXISTS Person_hasInterest_Tag;
DROP TABLE IF EXISTS Person_knows_Person;
DROP TABLE IF EXISTS Person_likes_Comment;
DROP TABLE IF EXISTS Person_likes_Post;
DROP TABLE IF EXISTS Person_studyAt_University;
DROP TABLE IF EXISTS Person_workAt_Company;
DROP TABLE IF EXISTS Post;
DROP TABLE IF EXISTS Post_hasTag_Tag;

-- processed inserts
DROP TABLE IF EXISTS Comment_Insert_Converted;
DROP TABLE IF EXISTS Forum_Insert_Converted;
DROP TABLE IF EXISTS Forum_hasMember_Person_Insert_Converted;
DROP TABLE IF EXISTS Person_Insert_Converted;
DROP TABLE IF EXISTS Person_knows_Person_Insert_Converted;
DROP TABLE IF EXISTS Person_likes_Comment_Insert_Converted;
DROP TABLE IF EXISTS Person_likes_Post_Insert_Converted;
DROP TABLE IF EXISTS Post_Insert_Converted;

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

-- vanilla inserts

CREATE TABLE Comment (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    content varchar(2000) NOT NULL,
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    LocationCountryId bigint NOT NULL,
    ParentPostId bigint,
    ParentCommentId bigint
);

CREATE TABLE Forum (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    title varchar(256) NOT NULL,
    ModeratorPersonId bigint -- can be null as its cardinality is 0..1
);

CREATE TABLE Post (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    imageFile varchar(40),
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    language varchar(40),
    content varchar(2000),
    length int NOT NULL,
    CreatorPersonId bigint NOT NULL,
    ContainerForumId bigint NOT NULL,
    LocationCountryId bigint NOT NULL
);

CREATE TABLE Person (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    firstName varchar(40) NOT NULL,
    lastName varchar(40) NOT NULL,
    gender varchar(40) NOT NULL,
    birthday date NOT NULL,
    locationIP varchar(40) NOT NULL,
    browserUsed varchar(40) NOT NULL,
    LocationCityId bigint NOT NULL,
    speaks varchar(640) NOT NULL,
    email varchar(8192) NOT NULL
);

CREATE TABLE Comment_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    CommentId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Post_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    PostId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Forum_hasMember_Person (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

CREATE TABLE Forum_hasTag_Tag (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Person_hasInterest_Tag (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    TagId bigint NOT NULL
);

CREATE TABLE Person_likes_Comment (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

CREATE TABLE Person_likes_Post (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

CREATE TABLE Person_studyAt_University (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    UniversityId bigint NOT NULL,
    classYear int NOT NULL
);

CREATE TABLE Person_workAt_Company (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CompanyId bigint NOT NULL,
    workFrom int NOT NULL
);

CREATE TABLE Person_knows_Person (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);

---- processed inserts

-- INS 7
CREATE TABLE Comment_Insert_Converted (
    creationDate timestamp with time zone NOT NULL,
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
CREATE TABLE Forum_Insert_Converted (
    creationDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
    title varchar(256) NOT NULL,
    ModeratorPersonId bigint,
    TagIds string
);

-- INS 6
CREATE TABLE Post_Insert_Converted (
    creationDate timestamp with time zone NOT NULL,
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
CREATE TABLE Person_Insert_Converted (
    creationDate timestamp with time zone NOT NULL,
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
CREATE TABLE Forum_hasMember_Person_Insert_Converted (
    creationDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

-- INS 3
CREATE TABLE Person_likes_Comment_Insert_Converted (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

-- INS 2
CREATE TABLE Person_likes_Post_Insert_Converted (
    creationDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

-- INS 8
CREATE TABLE Person_knows_Person_Insert_Converted (
    creationDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);

---- deletes

-- DEL 7
CREATE TABLE Comment_Delete (
    deletionDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY
);

-- DEL 4
CREATE TABLE Forum_Delete (
    deletionDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY,
);

-- DEL 6
CREATE TABLE Post_Delete (
    deletionDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY
);

-- DEL 1
CREATE TABLE Person_Delete (
    deletionDate timestamp with time zone NOT NULL,
    id bigint PRIMARY KEY
);

-- DEL 5
CREATE TABLE Forum_hasMember_Person_Delete (
    deletionDate timestamp with time zone NOT NULL,
    ForumId bigint NOT NULL,
    PersonId bigint NOT NULL
);

-- DEL 3
CREATE TABLE Person_likes_Comment_Delete (
    deletionDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    CommentId bigint NOT NULL
);

-- DEL 2
CREATE TABLE Person_likes_Post_Delete (
    deletionDate timestamp with time zone NOT NULL,
    PersonId bigint NOT NULL,
    PostId bigint NOT NULL
);

-- DEL 8
CREATE TABLE Person_knows_Person_Delete (
    deletionDate timestamp with time zone NOT NULL,
    Person1id bigint NOT NULL,
    Person2id bigint NOT NULL
);
