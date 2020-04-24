package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.ldbc.driver.validation.FilterAcceptanceType.*;

public final class InteractiveParamsFilter implements ParamsFilter {

    private final Set<String> multiResOps;
    private final Map<String, Integer> reqResPerOp;
    private final Set<String> enabledShortReadOps;

    public InteractiveParamsFilter(Set<String> multiResOps,
                                   Map<String, Integer> reqResPerOp,
                                   Set<String> enabledShortReadOps) {

        this.multiResOps = multiResOps;
        this.reqResPerOp = reqResPerOp;
        this.enabledShortReadOps = enabledShortReadOps;

    }

    @Override
    public boolean useOp(Operation op) {
        return reqResPerOp.containsKey(op.getClass().getName());
    }

    @Override
    public FilterResult useOpAndRes(Operation op, Object opRes) {

        String opType = op.getClass().getName();
        List<Operation> shortReads = new ArrayList<>();

        FilterResult filterResult;

        if (isGenerationComplete(reqResPerOp)){
            filterResult = new FilterResult(REJECT_AND_FINISH, shortReads);
        } else if ((multiResOps.contains(opType) && ((List) opRes).isEmpty()) ||
                (reqResPerOp.get(opType) == 0 && !isGenerationComplete(reqResPerOp))) {
            filterResult = new FilterResult(REJECT_AND_CONTINUE, shortReads);
        } else {
            reqResPerOp.put(opType, reqResPerOp.get(opType) - 1);
            shortReads = getValidShortReads(op);
            filterResult = new FilterResult(ACCEPT_AND_CONTINUE, shortReads);
        }

        return filterResult;

    }

    private List<Operation> getValidShortReads(Operation op) {

        List<Operation> proposedShortReads = new ArrayList<>(generateOperationsToInject(op));
        return  proposedShortReads.stream().filter(e -> (reqResPerOp.get(e.getClass().getName()) > 0)).collect(Collectors.toList());

    }

    private boolean isGenerationComplete(Map<String, Integer> reqResPerOpType) {

        for (Integer reqRes : reqResPerOpType.values()) {
            if (reqRes > 0) return false;
        }

        return true;
    }

    private List<Operation> generateOperationsToInject(Operation operation) {
        List<Operation> operationsToInject = new ArrayList<>();
        switch (operation.type()) {
            case LdbcUpdate1AddPerson.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate1AddPerson) operation).personId());
                injectShort2(operationsToInject, ((LdbcUpdate1AddPerson) operation).personId());
                injectShort3(operationsToInject, ((LdbcUpdate1AddPerson) operation).personId());
                break;
            }
            case LdbcUpdate2AddPostLike.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate2AddPostLike) operation).personId());
                injectShort2(operationsToInject, ((LdbcUpdate2AddPostLike) operation).personId());
                injectShort3(operationsToInject, ((LdbcUpdate2AddPostLike) operation).personId());
                injectShort4(operationsToInject, ((LdbcUpdate2AddPostLike) operation).postId());
                injectShort5(operationsToInject, ((LdbcUpdate2AddPostLike) operation).postId());
                injectShort6(operationsToInject, ((LdbcUpdate2AddPostLike) operation).postId());
                injectShort7(operationsToInject, ((LdbcUpdate2AddPostLike) operation).postId());
                break;
            }
            case LdbcUpdate3AddCommentLike.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).personId());
                injectShort2(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).personId());
                injectShort3(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).personId());
                injectShort4(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).commentId());
                injectShort5(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).commentId());
                injectShort6(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).commentId());
                injectShort7(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).commentId());
                break;
            }
            case LdbcUpdate4AddForum.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate4AddForum) operation).moderatorPersonId());
                injectShort2(operationsToInject, ((LdbcUpdate4AddForum) operation).moderatorPersonId());
                injectShort3(operationsToInject, ((LdbcUpdate4AddForum) operation).moderatorPersonId());
                break;
            }
            case LdbcUpdate5AddForumMembership.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate5AddForumMembership) operation).personId());
                injectShort2(operationsToInject, ((LdbcUpdate5AddForumMembership) operation).personId());
                injectShort3(operationsToInject, ((LdbcUpdate5AddForumMembership) operation).personId());
                break;
            }
            case LdbcUpdate6AddPost.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate6AddPost) operation).authorPersonId());
                injectShort2(operationsToInject, ((LdbcUpdate6AddPost) operation).authorPersonId());
                injectShort3(operationsToInject, ((LdbcUpdate6AddPost) operation).authorPersonId());
                injectShort4(operationsToInject, ((LdbcUpdate6AddPost) operation).postId());
                injectShort5(operationsToInject, ((LdbcUpdate6AddPost) operation).postId());
                injectShort6(operationsToInject, ((LdbcUpdate6AddPost) operation).postId());
                injectShort7(operationsToInject, ((LdbcUpdate6AddPost) operation).postId());
                break;
            }
            case LdbcUpdate7AddComment.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate7AddComment) operation).authorPersonId());
                injectShort2(operationsToInject, ((LdbcUpdate7AddComment) operation).authorPersonId());
                injectShort3(operationsToInject, ((LdbcUpdate7AddComment) operation).authorPersonId());
                injectShort4(operationsToInject, ((LdbcUpdate7AddComment) operation).commentId());
                injectShort5(operationsToInject, ((LdbcUpdate7AddComment) operation).commentId());
                injectShort6(operationsToInject, ((LdbcUpdate7AddComment) operation).commentId());
                injectShort7(operationsToInject, ((LdbcUpdate7AddComment) operation).commentId());
                break;
            }
            case LdbcUpdate8AddFriendship.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person1Id());
                injectShort2(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person1Id());
                injectShort3(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person1Id());
                injectShort1(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person2Id());
                injectShort2(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person2Id());
                injectShort3(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person2Id());
                break;
            }
        }
        return operationsToInject;
    }

    private void injectShort1(List<Operation> operationsToInject, long personId) {
        if (enabledShortReadOps.contains(LdbcShortQuery1PersonProfile.class.getName())) {
            operationsToInject.add(new LdbcShortQuery1PersonProfile(personId));
        }
    }

    private void injectShort2(List<Operation> operationsToInject, long personId) {
        if (enabledShortReadOps.contains(LdbcShortQuery2PersonPosts.class.getName())) {
            operationsToInject.add(new LdbcShortQuery2PersonPosts(personId, LdbcShortQuery2PersonPosts.DEFAULT_LIMIT));
        }
    }

    private void injectShort3(List<Operation> operationsToInject, long personId) {
        if (enabledShortReadOps.contains(LdbcShortQuery3PersonFriends.class.getName())) {
            operationsToInject.add(new LdbcShortQuery3PersonFriends(personId));
        }
    }

    private void injectShort4(List<Operation> operationsToInject, long messageId) {
        if (enabledShortReadOps.contains(LdbcShortQuery4MessageContent.class.getName())) {
            operationsToInject.add(new LdbcShortQuery4MessageContent(messageId));
        }
    }

    private void injectShort5(List<Operation> operationsToInject, long messageId) {
        if (enabledShortReadOps.contains(LdbcShortQuery5MessageCreator.class.getName())) {
            operationsToInject.add(new LdbcShortQuery5MessageCreator(messageId));
        }
    }

    private void injectShort6(List<Operation> operationsToInject, long messageId) {
        if (enabledShortReadOps.contains(LdbcShortQuery6MessageForum.class.getName())) {
            operationsToInject.add(new LdbcShortQuery6MessageForum(messageId));
        }
    }

    private void injectShort7(List<Operation> operationsToInject, long messageId) {
        if (enabledShortReadOps.contains(LdbcShortQuery7MessageReplies.class.getName())) {
            operationsToInject.add(
                    new LdbcShortQuery7MessageReplies(messageId)
            );
        }
    }
}
