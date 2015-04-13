package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload.DbValidationParametersFilter;
import com.ldbc.driver.Workload.DbValidationParametersFilterAcceptance;
import com.ldbc.driver.Workload.DbValidationParametersFilterResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class LdbcSnbInteractiveDbValidationParametersFilter implements DbValidationParametersFilter {
    private final Set<Class> multiResultOperations;
    private final Map<Class, Long> remainingRequiredResultsPerWriteType;
    private final Map<Class, Long> remainingRequiredResultsPerLongReadType;
    private final Set<Class> enabledShortReadOperationTypes;
    private long writeAddPersonOperationCount;
    private int uncompletedShortReads;

    LdbcSnbInteractiveDbValidationParametersFilter(Set<Class> multiResultOperations,
                                                   long writeAddPersonOperationCount,
                                                   Map<Class, Long> remainingRequiredResultsPerWriteType,
                                                   Map<Class, Long> remainingRequiredResultsPerLongReadType,
                                                   Set<Class> enabledShortReadOperationTypes) {
        this.multiResultOperations = multiResultOperations;
        this.writeAddPersonOperationCount = writeAddPersonOperationCount;
        this.remainingRequiredResultsPerWriteType = remainingRequiredResultsPerWriteType;
        this.remainingRequiredResultsPerLongReadType = remainingRequiredResultsPerLongReadType;
        this.enabledShortReadOperationTypes = enabledShortReadOperationTypes;
        this.uncompletedShortReads = 0;
    }

    @Override
    public boolean useOperation(Operation operation) {
        Class operationType = operation.getClass();

        if (enabledShortReadOperationTypes.contains(operationType)) {
            return true;
        } else if (operationType.equals(LdbcUpdate1AddPerson.class)) {
            return writeAddPersonOperationCount > 0;
        } else if (remainingRequiredResultsPerWriteType.containsKey(operationType)) {
            return false == haveCompletedAllRequiredResultsPerOperationType(remainingRequiredResultsPerWriteType);
        } else if (remainingRequiredResultsPerLongReadType.containsKey(operationType)) {
            return remainingRequiredResultsPerLongReadType.get(operationType) > 0;
        } else {
            // disabled operation
            return false;
        }
    }

    @Override
    public DbValidationParametersFilterResult useOperationAndResultForValidation(Operation operation,
                                                                                 Object operationResult) {
        Class operationType = operation.getClass();
        List<Operation> injectedOperations = new ArrayList<>();

        // do not use empty results for validation
        if (multiResultOperations.contains(operationType) && ((List) operationResult).isEmpty()) {
            return new DbValidationParametersFilterResult(DbValidationParametersFilterAcceptance.REJECT_AND_CONTINUE, injectedOperations);
        }

        injectedOperations.addAll(generateOperationsToInject(operation));
        uncompletedShortReads += injectedOperations.size();

        if (operationType.equals(LdbcUpdate1AddPerson.class)) {
            // writes do not return anything, but they should be executed and some default result needs to be stored in the validation parameters
            writeAddPersonOperationCount--;
        } else if (enabledShortReadOperationTypes.contains(operationType)) {
            // keep track of how many injected operations have completed (only short reads are injected)
            uncompletedShortReads--;
        } else if (remainingRequiredResultsPerWriteType.containsKey(operationType)) {
            // decrement count for write operation type
            remainingRequiredResultsPerWriteType.put(operationType, Math.max(0, remainingRequiredResultsPerWriteType.get(operationType) - 1));
        } else if (remainingRequiredResultsPerLongReadType.containsKey(operationType)) {
            // decrement count for long read operation type
            remainingRequiredResultsPerLongReadType.put(operationType, remainingRequiredResultsPerLongReadType.get(operationType) - 1);
        } else {
            throw new RuntimeException("Unexpected operation type: " + operationType.getSimpleName());
        }

        if (validationParameterGenerationFinished()) {
            return new DbValidationParametersFilterResult(DbValidationParametersFilterAcceptance.ACCEPT_AND_FINISH, injectedOperations);
        } else {
            return new DbValidationParametersFilterResult(DbValidationParametersFilterAcceptance.ACCEPT_AND_CONTINUE, injectedOperations);
        }
    }

    private boolean validationParameterGenerationFinished() {
        // check that all writes have completed
        if (false == haveCompletedAllRequiredResultsPerOperationType(remainingRequiredResultsPerWriteType)) {
            return false;
        }
        // check that all long reads have completed
        if (false == haveCompletedAllRequiredResultsPerOperationType(remainingRequiredResultsPerLongReadType)) {
            return false;
        }
        // check that all Add Person writes have completed
        if (writeAddPersonOperationCount > 0) {
            return false;
        }
        // check that all short reads have completed
        if (uncompletedShortReads > 0) {
            return false;
        }
        // we're done
        return true;
    }

    private boolean haveCompletedAllRequiredResultsPerOperationType(Map<Class, Long> requiredResultsPerOperationType) {
        for (Long value : requiredResultsPerOperationType.values()) {
            if (value > 0) return false;
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
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery1PersonProfile.class)) {
            operationsToInject.add(
                    new LdbcShortQuery1PersonProfile(personId)
            );
        }
    }

    private void injectShort2(List<Operation> operationsToInject, long personId) {
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery2PersonPosts.class)) {
            operationsToInject.add(
                    new LdbcShortQuery2PersonPosts(personId, LdbcShortQuery2PersonPosts.DEFAULT_LIMIT)
            );
        }
    }

    private void injectShort3(List<Operation> operationsToInject, long personId) {
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery3PersonFriends.class)) {
            operationsToInject.add(
                    new LdbcShortQuery3PersonFriends(personId)
            );
        }
    }

    private void injectShort4(List<Operation> operationsToInject, long messageId) {
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery4MessageContent.class)) {
            operationsToInject.add(
                    new LdbcShortQuery4MessageContent(messageId)
            );
        }
    }

    private void injectShort5(List<Operation> operationsToInject, long messageId) {
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery5MessageCreator.class)) {
            operationsToInject.add(
                    new LdbcShortQuery5MessageCreator(messageId)
            );
        }
    }

    private void injectShort6(List<Operation> operationsToInject, long messageId) {
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery6MessageForum.class)) {
            operationsToInject.add(
                    new LdbcShortQuery6MessageForum(messageId)
            );
        }
    }

    private void injectShort7(List<Operation> operationsToInject, long messageId) {
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery7MessageReplies.class)) {
            operationsToInject.add(
                    new LdbcShortQuery7MessageReplies(messageId)
            );
        }
    }
}
