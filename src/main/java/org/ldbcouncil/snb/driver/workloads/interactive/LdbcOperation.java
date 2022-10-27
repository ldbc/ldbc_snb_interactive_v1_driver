package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcOperation.java
 * Wrapper abstract class to define the types of Operation classes the
 * LDBC SNB Interactive workload implements.
 */

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonTypeInfo(use=Id.NAME, include=As.WRAPPER_OBJECT)
@JsonSubTypes( {
    @JsonSubTypes.Type(LdbcQuery1.class),
    @JsonSubTypes.Type(LdbcQuery2.class),
    @JsonSubTypes.Type(LdbcQuery3a.class),
    @JsonSubTypes.Type(LdbcQuery3b.class),
    @JsonSubTypes.Type(LdbcQuery4.class),
    @JsonSubTypes.Type(LdbcQuery5.class),
    @JsonSubTypes.Type(LdbcQuery6.class),
    @JsonSubTypes.Type(LdbcQuery7.class),
    @JsonSubTypes.Type(LdbcQuery8.class),
    @JsonSubTypes.Type(LdbcQuery9.class),
    @JsonSubTypes.Type(LdbcQuery10.class),
    @JsonSubTypes.Type(LdbcQuery11.class),
    @JsonSubTypes.Type(LdbcQuery12.class),
    @JsonSubTypes.Type(LdbcQuery13a.class),
    @JsonSubTypes.Type(LdbcQuery13b.class),
    @JsonSubTypes.Type(LdbcQuery14a.class),
    @JsonSubTypes.Type(LdbcQuery14b.class),
    @JsonSubTypes.Type(LdbcShortQuery1PersonProfile.class),
    @JsonSubTypes.Type(LdbcShortQuery2PersonPosts.class),
    @JsonSubTypes.Type(LdbcShortQuery3PersonFriends.class),
    @JsonSubTypes.Type(LdbcShortQuery4MessageContent.class),
    @JsonSubTypes.Type(LdbcShortQuery5MessageCreator.class),
    @JsonSubTypes.Type(LdbcShortQuery6MessageForum.class),
    @JsonSubTypes.Type(LdbcShortQuery7MessageReplies.class),
    @JsonSubTypes.Type(LdbcInsert1AddPerson.class),
    @JsonSubTypes.Type(LdbcInsert2AddPostLike.class),
    @JsonSubTypes.Type(LdbcInsert3AddCommentLike.class),
    @JsonSubTypes.Type(LdbcInsert4AddForum.class),
    @JsonSubTypes.Type(LdbcInsert5AddForumMembership.class),
    @JsonSubTypes.Type(LdbcInsert6AddPost.class),
    @JsonSubTypes.Type(LdbcInsert7AddComment.class),
    @JsonSubTypes.Type(LdbcInsert8AddFriendship.class),
    @JsonSubTypes.Type(LdbcDelete1RemovePerson.class),
    @JsonSubTypes.Type(LdbcDelete2RemovePostLike.class),
    @JsonSubTypes.Type(LdbcDelete3RemoveCommentLike.class),
    @JsonSubTypes.Type(LdbcDelete4RemoveForum.class),
    @JsonSubTypes.Type(LdbcDelete5RemoveForumMembership.class),
    @JsonSubTypes.Type(LdbcDelete6RemovePostThread.class),
    @JsonSubTypes.Type(LdbcDelete7RemoveCommentSubthread.class),
    @JsonSubTypes.Type(LdbcDelete8RemoveFriendship.class)
})
public abstract class LdbcOperation<LDBC_RESULT_TYPE> extends Operation<LDBC_RESULT_TYPE> {
    
}
