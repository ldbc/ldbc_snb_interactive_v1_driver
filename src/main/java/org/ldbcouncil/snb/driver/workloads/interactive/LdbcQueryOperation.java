package org.ldbcouncil.snb.driver.workloads.interactive;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.ldbcouncil.snb.driver.Operation;

import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id.DEDUCTION;

import java.io.IOException;

@JsonTypeInfo(use=DEDUCTION)
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
})
public abstract class LdbcQueryOperation<RESULT_TYPE> extends Operation{
    

}
