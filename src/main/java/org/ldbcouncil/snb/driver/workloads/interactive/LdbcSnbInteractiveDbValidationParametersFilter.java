package org.ldbcouncil.snb.driver.workloads.interactive;
/**
 * LdbcSnbInteractiveDbValidationParametersFilter.java
 * IF update queries are disabled, then short reads are dependent on the results of complex queries. 
 * IF update queries are enabled,  then short reads are dependent on update queries.
 */

import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.Workload.DbValidationParametersFilter;
import org.ldbcouncil.snb.driver.Workload.DbValidationParametersFilterAcceptance;
import org.ldbcouncil.snb.driver.Workload.DbValidationParametersFilterResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class LdbcSnbInteractiveDbValidationParametersFilter implements DbValidationParametersFilter {
    private final Map<Class, Long> remainingRequiredResultsPerWriteType;
    private final Map<Class, Long> remainingRequiredResultsPerLongReadType;
    private final Set<Class> enabledShortReadOperationTypes;
    private int uncompletedShortReads;

    LdbcSnbInteractiveDbValidationParametersFilter(
        Map<Class, Long> remainingRequiredResultsPerWriteType,
        Map<Class, Long> remainingRequiredResultsPerLongReadType,
        Set<Class> enabledShortReadOperationTypes) {
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
        } else if (remainingRequiredResultsPerWriteType.containsKey(operationType)) {
            return !haveCompletedAllRequiredResultsPerOperationType(remainingRequiredResultsPerWriteType);
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

        injectedOperations.addAll(generateOperationsToInject(operation));
        uncompletedShortReads += injectedOperations.size();

        if (enabledShortReadOperationTypes.contains(operationType)) {
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
        if (!haveCompletedAllRequiredResultsPerOperationType(remainingRequiredResultsPerWriteType)) {
            return false;
        }
        // check that all long reads have completed
        if (!haveCompletedAllRequiredResultsPerOperationType(remainingRequiredResultsPerLongReadType)) {
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

    /**
     * Inject short reads after a LdbcUpdate operation is scheduled.
     * @param operation The update operation
     * @return List of short reads operations
     */
    private List<Operation> generateOperationsToInject(Operation operation) {
        List<Operation> operationsToInject = new ArrayList<>();
        switch (operation.type()) {
            case LdbcUpdate1AddPerson.TYPE: {
                LdbcUpdate1AddPerson updateOperation = (LdbcUpdate1AddPerson) operation;
                injectPersonShorts(operationsToInject, updateOperation.getPersonId());
                break;
            }
            case LdbcUpdate2AddPostLike.TYPE: {
                LdbcUpdate2AddPostLike updateOperation = (LdbcUpdate2AddPostLike) operation;
                injectAllShorts(operationsToInject, updateOperation.getPersonId(), updateOperation.getPostId());
                break;
            }
            case LdbcUpdate3AddCommentLike.TYPE: {
                LdbcUpdate3AddCommentLike updateOperation = (LdbcUpdate3AddCommentLike) operation;
                injectAllShorts(operationsToInject, updateOperation.getPersonId(), updateOperation.getCommentId());
                break;
            }
            case LdbcUpdate4AddForum.TYPE: {
                LdbcUpdate4AddForum updateOperation = (LdbcUpdate4AddForum) operation;
                injectPersonShorts(operationsToInject, updateOperation.getModeratorPersonId());
                break;
            }
            case LdbcUpdate5AddForumMembership.TYPE: {
                LdbcUpdate5AddForumMembership updateOperation = (LdbcUpdate5AddForumMembership) operation;
                injectPersonShorts(operationsToInject, updateOperation.getPersonId());
                break;
            }
            case LdbcUpdate6AddPost.TYPE: {
                LdbcUpdate6AddPost updateOperation = (LdbcUpdate6AddPost) operation;
                injectAllShorts(operationsToInject, updateOperation.getAuthorPersonId(), updateOperation.getPostId());
                break;
            }
            case LdbcUpdate7AddComment.TYPE: {
                LdbcUpdate7AddComment updateOperation = (LdbcUpdate7AddComment) operation;
                injectAllShorts(operationsToInject, updateOperation.getAuthorPersonId(), updateOperation.getCommentId());
                break;
            }
            case LdbcUpdate8AddFriendship.TYPE: {
                LdbcUpdate8AddFriendship updateOperation = (LdbcUpdate8AddFriendship) operation;
                injectPersonShorts(operationsToInject, updateOperation.getPerson1Id());
                injectPersonShorts(operationsToInject, updateOperation.getPerson2Id());
                break;
            }
        }
        return operationsToInject;
    }

    private void injectAllShorts(List<Operation> operationsToInject, long personId, long messageId)
    {
        injectShort1(operationsToInject, personId);
        injectShort2(operationsToInject, personId);
        injectShort3(operationsToInject, personId);
        injectShort4(operationsToInject, messageId);
        injectShort5(operationsToInject, messageId);
        injectShort6(operationsToInject, messageId);
        injectShort7(operationsToInject, messageId);
    }

    private void injectPersonShorts(List<Operation> operationsToInject, long personId) {
        injectShort1(operationsToInject, personId);
        injectShort2(operationsToInject, personId);
        injectShort3(operationsToInject, personId);
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
