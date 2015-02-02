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
    private final Map<Class, Long> remainingRequiredResultsPerOperationType;
    private final Set<Class> enabledShortReadOperationTypes;
    private final Set<Class> enabledWriteOperationTypes;
    private final Object noResultDefaultResult;

    private int writeAddPersonOperationCount;
    private int uncompletedShortReads;

    LdbcSnbInteractiveDbValidationParametersFilter(Set<Class> multiResultOperations,
                                                   Map<Class, Long> remainingRequiredResultsPerOperationType,
                                                   Set<Class> enabledShortReadOperationTypes,
                                                   Set<Class> enabledWriteOperationTypes,
                                                   int writeAddPersonOperationCount,
                                                   Object noResultDefaultResult) {
        this.multiResultOperations = multiResultOperations;
        this.remainingRequiredResultsPerOperationType = remainingRequiredResultsPerOperationType;
        this.enabledShortReadOperationTypes = enabledShortReadOperationTypes;
        this.enabledWriteOperationTypes = enabledWriteOperationTypes;
        this.writeAddPersonOperationCount = writeAddPersonOperationCount;
        this.uncompletedShortReads = 0;
        this.noResultDefaultResult=noResultDefaultResult;
    }

    @Override
    public boolean useOperation(Operation<?> operation) {
        Class operationType = operation.getClass();
        if (operationType.equals(LdbcUpdate1AddPerson.class)) {
            return writeAddPersonOperationCount > 0;
        } else if (enabledShortReadOperationTypes.contains(operationType)) {
            return true;
        } else {
            return remainingRequiredResultsPerOperationType.containsKey(operationType) && remainingRequiredResultsPerOperationType.get(operationType) > 0;
        }
    }

    @Override
    public DbValidationParametersFilterResult useOperationAndResultForValidation(Operation<?> operation,
                                                                                 Object operationResult) {
        Class operationType = operation.getClass();
        List<Operation> injectedOperations = generateOperationsToInject(operation);
        uncompletedShortReads += injectedOperations.size();

        // do not use empty results for validation
        if (multiResultOperations.contains(operationType) && ((List) operationResult).isEmpty()) {
            return new DbValidationParametersFilterResult(DbValidationParametersFilterAcceptance.REJECT_AND_CONTINUE, injectedOperations);
        }

        if (operationType.equals(LdbcUpdate1AddPerson.class)) {
            // updates do not return anything, but they should be executed and some default result needs to be stored in the validation parameters
            writeAddPersonOperationCount--;
        } else if (enabledShortReadOperationTypes.contains(operationType)) {
            uncompletedShortReads--;
        } else {
            // decrement count for operation type
            remainingRequiredResultsPerOperationType.put(operationType, remainingRequiredResultsPerOperationType.get(operationType) - 1);
        }

        if (validationParameterGenerationFinished(remainingRequiredResultsPerOperationType)) {
            return new DbValidationParametersFilterResult(DbValidationParametersFilterAcceptance.ACCEPT_AND_FINISH, injectedOperations);
        } else {
            return new DbValidationParametersFilterResult(DbValidationParametersFilterAcceptance.ACCEPT_AND_CONTINUE, injectedOperations);
        }
    }

    @Override
    public Object curateResult(Operation operation, Object result) {
        return (enabledWriteOperationTypes.contains(operation.getClass()))
                ? noResultDefaultResult
                : result;
    }

    private boolean validationParameterGenerationFinished(Map<Class, Long> requiredResultsPerOperationType) {
        // check that all long reads have completed
        for (Long value : requiredResultsPerOperationType.values()) {
            if (0 < value) return false;
        }
        // check that all Add Person writes have completed
        if (writeAddPersonOperationCount > 0) return false;
        // check that all short reads have completed
        if (uncompletedShortReads > 0) return false;
        // we're done
        return true;
    }

    private List<Operation> generateOperationsToInject(Operation operation) {
        List<Operation> operationsToInject = new ArrayList<>();
        switch (operation.type()) {
            case LdbcUpdate1AddPerson.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate1AddPerson) operation).personId());
                injectShort2(operationsToInject, ((LdbcUpdate1AddPerson) operation).personId());
                injectShort3(operationsToInject, ((LdbcUpdate1AddPerson) operation).personId());
            }
            case LdbcUpdate2AddPostLike.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate2AddPostLike) operation).personId());
                injectShort2(operationsToInject, ((LdbcUpdate2AddPostLike) operation).personId());
                injectShort3(operationsToInject, ((LdbcUpdate2AddPostLike) operation).personId());
                injectShort4(operationsToInject, ((LdbcUpdate2AddPostLike) operation).postId());
                injectShort5(operationsToInject, ((LdbcUpdate2AddPostLike) operation).postId());
                injectShort6(operationsToInject, ((LdbcUpdate2AddPostLike) operation).postId());
                injectShort7(operationsToInject, ((LdbcUpdate2AddPostLike) operation).postId());
            }
            case LdbcUpdate3AddCommentLike.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).personId());
                injectShort2(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).personId());
                injectShort3(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).personId());
                injectShort4(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).commentId());
                injectShort5(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).commentId());
                injectShort6(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).commentId());
                injectShort7(operationsToInject, ((LdbcUpdate3AddCommentLike) operation).commentId());
            }
            case LdbcUpdate4AddForum.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate4AddForum) operation).moderatorPersonId());
                injectShort2(operationsToInject, ((LdbcUpdate4AddForum) operation).moderatorPersonId());
                injectShort3(operationsToInject, ((LdbcUpdate4AddForum) operation).moderatorPersonId());
            }
            case LdbcUpdate5AddForumMembership.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate5AddForumMembership) operation).personId());
                injectShort2(operationsToInject, ((LdbcUpdate5AddForumMembership) operation).personId());
                injectShort3(operationsToInject, ((LdbcUpdate5AddForumMembership) operation).personId());
            }
            case LdbcUpdate6AddPost.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate6AddPost) operation).authorPersonId());
                injectShort2(operationsToInject, ((LdbcUpdate6AddPost) operation).authorPersonId());
                injectShort3(operationsToInject, ((LdbcUpdate6AddPost) operation).authorPersonId());
                injectShort4(operationsToInject, ((LdbcUpdate6AddPost) operation).postId());
                injectShort5(operationsToInject, ((LdbcUpdate6AddPost) operation).postId());
                injectShort6(operationsToInject, ((LdbcUpdate6AddPost) operation).postId());
                injectShort7(operationsToInject, ((LdbcUpdate6AddPost) operation).postId());
            }
            case LdbcUpdate7AddComment.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate7AddComment) operation).authorPersonId());
                injectShort2(operationsToInject, ((LdbcUpdate7AddComment) operation).authorPersonId());
                injectShort3(operationsToInject, ((LdbcUpdate7AddComment) operation).authorPersonId());
                injectShort4(operationsToInject, ((LdbcUpdate7AddComment) operation).commentId());
                injectShort5(operationsToInject, ((LdbcUpdate7AddComment) operation).commentId());
                injectShort6(operationsToInject, ((LdbcUpdate7AddComment) operation).commentId());
                injectShort7(operationsToInject, ((LdbcUpdate7AddComment) operation).commentId());
            }
            case LdbcUpdate8AddFriendship.TYPE: {
                injectShort1(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person1Id());
                injectShort2(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person1Id());
                injectShort3(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person1Id());
                injectShort1(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person2Id());
                injectShort2(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person2Id());
                injectShort3(operationsToInject, ((LdbcUpdate8AddFriendship) operation).person2Id());
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
                    new LdbcShortQuery2PersonPosts(personId)
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
