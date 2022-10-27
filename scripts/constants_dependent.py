schema_columns = {
    "Comment_Insert":['creationDate', '0', 'id', 'locationIP', 'browserUsed', 'content', 'length', 'CreatorPersonId', 'LocationCountryId', 'ParentPostId', 'ParentCommentId', 'TagIds'],
    "Forum_Insert":['creationDate', '0', 'id', 'title', 'ModeratorPersonid', 'TagIds'],
    "Forum_hasMember_Person_Insert":['creationDate', '0', 'PersonId', 'ForumId'],
    "Person_Insert":['creationDate', '0', 'id', 'firstName', 'lastName', 'gender', 'birthday', 'locationIP', 'browserUsed', 'LocationCityId', 'speaks', 'email', 'tagIds', 'studyAt', 'workAt'],
    "Person_knows_Person_Insert":['creationDate', '0', 'Person1id', 'Person2id'],
    "Person_likes_Comment_Insert":['creationDate', '0', 'PersonId', 'CommentId'],
    "Person_likes_Post_Insert":['creationDate', '0', 'PersonId', 'PostId'],
    "Post_Insert":['creationDate', '0', 'id', 'imageFile', 'locationIP', 'browserUsed', 'language', 'content', 'length', 'CreatorPersonId', 'ContainerForumId', 'LocationCountryId', 'TagIds'],
    
    "Comment_Delete":['deletionDate', '0', 'id'],
    "Forum_Delete":['deletionDate', '0', 'id'],
    "Forum_hasMember_Person_Delete":['deletionDate', '0','ForumId', 'PersonId'],
    "Person_Delete":['deletionDate', '0', 'id'],
    "Person_knows_Person_Delete":['deletionDate', '0','Person1id', 'Person2id'],
    "Person_likes_Comment_Delete":['deletionDate', '0', 'PersonId', 'CommentId'],
    "Person_likes_Post_Delete":['deletionDate', '0', 'PersonId', 'PostId'],
    "Post_Delete":['deletionDate', '0', 'id'],
}

dependent_entity_map = {
    # Inserts
    "Comment_Insert":{
        "entity":["Person", "Post", "Forum"],
        "eventColumns": ["CreatorPersonId", "ParentPostId", "ParentCommentId"],
        "entityColumns":["id", "id", "id"],
        "matchColumns": ["id"],
        "dateColumn":"creationDate"
    },
    "Forum_Insert":{
        "entity":["Person"],
        "eventColumns": ["ModeratorPersonId"],
        "entityColumns":["id"],
        "matchColumns": ["id"],
        "dateColumn":"creationDate"
    },
    "Forum_hasMember_Person_Insert":{
        "entity":["Person", "Forum"],
        "eventColumns": ["PersonId", "ForumId"],
        "entityColumns":["id", "id"],
        "matchColumns":["PersonId", "ForumId"],
        "dateColumn":"creationDate"
    },
    # "Person_Insert":[], # uses default value
    "Person_knows_Person_Insert":{
        "entity":["Person", "Person"],
        "eventColumns": ["Person1id", "Person2id"],
        "entityColumns":["id", "id"],
        "matchColumns": ["Person1id", "Person2id"],
        "dateColumn":"creationDate"
    },
    "Person_likes_Comment_Insert":{
        "entity":["Person", "Comment"],
        "eventColumns": ["PersonId", "CommentId"],
        "entityColumns":["id", "id"],
        "matchColumns":["PersonId", "CommentId"],
        "dateColumn":"creationDate"
    },
    "Person_likes_Post_Insert":{
        "entity":["Person", "Post"],
        "eventColumns": ["PersonId", "PostId"],
        "entityColumns":["id", "id"],
        "matchColumns":["PersonId", "PostId"],
        "dateColumn":"creationDate"
    },
    "Post_Insert":{
        "entity":["Person", "Post"],
        "eventColumns": ["CreatorPersonId", "ContainerForumId"],
        "entityColumns":["id", "id"],
        "matchColumns":["CreatorPersonId", "ContainerForumId"],
        "dateColumn":"creationDate"
    },
    # Deletes
    "Comment_Delete":{
        "entity":["Comment",],
        "eventColumns": ["id"],
        "entityColumns":["id"],
        "matchColumns": ["id"],
        "dateColumn":"creationDate"
    },
    "Forum_Delete":{
        "entity":["Forum",],
        "eventColumns": ["id"],
        "entityColumns":["id"],
        "matchColumns": ["id"],
        "dateColumn":"creationDate"
    },
    "Forum_hasMember_Person_Delete":{
        "entity":["Forum","Person"],
        "eventColumns": ["ForumId", "PersonId"],
        "entityColumns":["id", "id"],
        "matchColumns": ["ForumId", "PersonId"],
        "dateColumn":"creationDate"
    },
    "Person_Delete":{
        "entity":["Person",],
        "eventColumns": ["id"],
        "entityColumns":["id"],
        "matchColumns": ["id"],
        "dateColumn":"creationDate"
    },
    "Person_knows_Person_Delete":{
        "entity":["Person", "Person"],
        "eventColumns": ["Person1id", "Person2id"],
        "entityColumns":["id", "id"],
        "matchColumns":["Person1id", "Person2id"],
        "dateColumn":"creationDate"
    },
    "Person_likes_Comment_Delete":{
        "entity":["Person", "Comment"],
        "eventColumns": ["PersonId", "CommentId"],
        "entityColumns":["id", "id"],
        "matchColumns": ["PersonId", "CommentId"],
        "dateColumn":"creationDate"
    },
    "Person_likes_Post_Delete":{
        "entity":["Person", "Post"],
        "eventColumns": ["PersonId", "PostId"],
        "entityColumns":["id", "id"],
        "matchColumns": ["PersonId", "PostId"],
        "dateColumn":"creationDate"
    },
    "Post_Delete":{
        "entity":["Post"],
        "eventColumns": ["id"],
        "entityColumns":["id"],
        "matchColumns": ["id"],
        "dateColumn":"creationDate"
    }
}
