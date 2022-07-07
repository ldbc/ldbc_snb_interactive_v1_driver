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
    @JsonSubTypes.Type(LdbcQuery3.class),
    @JsonSubTypes.Type(LdbcQuery4.class),
    @JsonSubTypes.Type(LdbcQuery5.class),
    @JsonSubTypes.Type(LdbcQuery6.class),
    @JsonSubTypes.Type(LdbcQuery7.class),
    @JsonSubTypes.Type(LdbcQuery8.class),
    @JsonSubTypes.Type(LdbcQuery9.class),
    @JsonSubTypes.Type(LdbcQuery10.class),
    @JsonSubTypes.Type(LdbcQuery11.class),
    @JsonSubTypes.Type(LdbcQuery12.class),
    @JsonSubTypes.Type(LdbcQuery13.class),
    @JsonSubTypes.Type(LdbcQuery14.class),
    @JsonSubTypes.Type(LdbcShortQuery1PersonProfile.class),
    @JsonSubTypes.Type(LdbcShortQuery2PersonPosts.class),
    @JsonSubTypes.Type(LdbcShortQuery3PersonFriends.class),
    @JsonSubTypes.Type(LdbcShortQuery4MessageContent.class),
    @JsonSubTypes.Type(LdbcShortQuery5MessageCreator.class),
    @JsonSubTypes.Type(LdbcShortQuery6MessageForum.class),
    @JsonSubTypes.Type(LdbcShortQuery7MessageReplies.class),
    @JsonSubTypes.Type(LdbcUpdate1AddPerson.class),
    @JsonSubTypes.Type(LdbcUpdate2AddPostLike.class),
    @JsonSubTypes.Type(LdbcUpdate3AddCommentLike.class),
    @JsonSubTypes.Type(LdbcUpdate4AddForum.class),
    @JsonSubTypes.Type(LdbcUpdate5AddForumMembership.class),
    @JsonSubTypes.Type(LdbcUpdate6AddPost.class),
    @JsonSubTypes.Type(LdbcUpdate7AddComment.class),
    @JsonSubTypes.Type(LdbcUpdate8AddFriendship.class),
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