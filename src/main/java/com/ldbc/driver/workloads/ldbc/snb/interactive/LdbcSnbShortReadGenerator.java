package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.ImmutableList;
import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class LdbcSnbShortReadGenerator implements ChildOperationGenerator {
    private final double initialProbability;
    private final double probabilityDegradationFactor;
    private final double minimumProbability;
    private final LdbcShortQueryFactory[] personOperationFactories;
    private final LdbcShortQueryFactory[] messageOperationFactories;
    private final int personOperationFactoriesCount;
    private final int messageOperationFactoriesCount;
    private final int operationFactoriesCount;
    private final RandomDataGenerator random;
    private final long interleaveAsMilli;

    public LdbcSnbShortReadGenerator(double initialProbability,
                                     double probabilityDegradationFactor,
                                     double minimumProbability,
                                     long interleaveAsMilli,
                                     Set<Class> enabledShortReadOperationTypes,
                                     double compressionRatio) {
        this.initialProbability = initialProbability;
        this.probabilityDegradationFactor = probabilityDegradationFactor;
        this.minimumProbability = minimumProbability;
        this.random = new RandomDataGeneratorFactory().newRandom(42l);

        List<LdbcShortQueryFactory> personOperationFactoriesList = new ArrayList<>();
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery1PersonProfile.class))
            personOperationFactoriesList.add(new LdbcShortQuery1Factory());
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery2PersonPosts.class))
            personOperationFactoriesList.add(new LdbcShortQuery2Factory());
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery3PersonFriends.class))
            personOperationFactoriesList.add(new LdbcShortQuery3Factory());
        this.personOperationFactories = personOperationFactoriesList.toArray(new LdbcShortQueryFactory[personOperationFactoriesList.size()]);
        this.personOperationFactoriesCount = personOperationFactories.length;

        List<LdbcShortQueryFactory> messageOperationFactoriesList = new ArrayList<>();
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery4MessageContent.class))
            messageOperationFactoriesList.add(new LdbcShortQuery4Factory());
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery5MessageCreator.class))
            messageOperationFactoriesList.add(new LdbcShortQuery5Factory());
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery6MessageForum.class))
            messageOperationFactoriesList.add(new LdbcShortQuery6Factory());
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery7MessageReplies.class))
            messageOperationFactoriesList.add(new LdbcShortQuery7Factory());
        this.messageOperationFactories = messageOperationFactoriesList.toArray(new LdbcShortQueryFactory[messageOperationFactoriesList.size()]);
        this.messageOperationFactoriesCount = messageOperationFactories.length;

        this.operationFactoriesCount = personOperationFactoriesCount + messageOperationFactoriesCount;

        this.interleaveAsMilli = Math.round(Math.ceil(compressionRatio * interleaveAsMilli));
    }

    @Override
    public double initialState() {
        return initialProbability;
    }

    @Override
    public Operation<?> nextOperation(double state, Operation operation, Object result) throws WorkloadException {
        if (state > minimumProbability) {
            switch (operation.type()) {
                case LdbcQuery1.TYPE: {
                    if (0 == personOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcQuery1Result> typedResult = (List<LdbcQuery1Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == personOperationFactoriesCount) ? 0 : random.nextInt(0, 2);
                        return personOperationFactories[operationFactoryIndex].create(
                                typedResult.get(randomResultIndex).friendId(),
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery2.TYPE: {
                    List<LdbcQuery2Result> typedResult = (List<LdbcQuery2Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == operationFactoriesCount) ? 0 : random.nextInt(0, operationFactoriesCount - 1);
                        if (operationFactoryIndex < personOperationFactoriesCount) {
                            return personOperationFactories[operationFactoryIndex].create(
                                    typedResult.get(randomResultIndex).personId(),
                                    operation.scheduledStartTimeAsMilli()
                            );
                        } else {
                            return messageOperationFactories[operationFactoryIndex - 3].create(
                                    typedResult.get(randomResultIndex).postOrCommentId(),
                                    operation.scheduledStartTimeAsMilli()
                            );
                        }
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery3.TYPE: {
                    if (0 == personOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcQuery3Result> typedResult = (List<LdbcQuery3Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == personOperationFactoriesCount) ? 0 : random.nextInt(0, 2);
                        return personOperationFactories[operationFactoryIndex].create(
                                typedResult.get(randomResultIndex).personId(),
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery7.TYPE: {
                    List<LdbcQuery7Result> typedResult = (List<LdbcQuery7Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == operationFactoriesCount) ? 0 : random.nextInt(0, operationFactoriesCount - 1);
                        if (operationFactoryIndex < personOperationFactoriesCount) {
                            return personOperationFactories[operationFactoryIndex].create(
                                    typedResult.get(randomResultIndex).personId(),
                                    operation.scheduledStartTimeAsMilli()
                            );
                        } else {
                            return messageOperationFactories[operationFactoryIndex - 3].create(
                                    typedResult.get(randomResultIndex).commentOrPostId(),
                                    operation.scheduledStartTimeAsMilli()
                            );
                        }
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery8.TYPE: {
                    List<LdbcQuery8Result> typedResult = (List<LdbcQuery8Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == operationFactoriesCount) ? 0 : random.nextInt(0, operationFactoriesCount - 1);
                        if (operationFactoryIndex < personOperationFactoriesCount) {
                            return personOperationFactories[operationFactoryIndex].create(
                                    typedResult.get(randomResultIndex).personId(),
                                    operation.scheduledStartTimeAsMilli()
                            );
                        } else {
                            return messageOperationFactories[operationFactoryIndex - 3].create(
                                    typedResult.get(randomResultIndex).commentId(),
                                    operation.scheduledStartTimeAsMilli()
                            );
                        }
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery9.TYPE: {
                    List<LdbcQuery9Result> typedResult = (List<LdbcQuery9Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == operationFactoriesCount) ? 0 : random.nextInt(0, operationFactoriesCount - 1);
                        if (operationFactoryIndex < personOperationFactoriesCount) {
                            return personOperationFactories[operationFactoryIndex].create(
                                    typedResult.get(randomResultIndex).personId(),
                                    operation.scheduledStartTimeAsMilli()
                            );
                        } else {
                            return messageOperationFactories[operationFactoryIndex - 3].create(
                                    typedResult.get(randomResultIndex).commentOrPostId(),
                                    operation.scheduledStartTimeAsMilli()
                            );
                        }
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery10.TYPE: {
                    if (0 == personOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcQuery10Result> typedResult = (List<LdbcQuery10Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == personOperationFactoriesCount) ? 0 : random.nextInt(0, 2);
                        return personOperationFactories[operationFactoryIndex].create(
                                typedResult.get(randomResultIndex).personId(),
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery11.TYPE: {
                    if (0 == personOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcQuery11Result> typedResult = (List<LdbcQuery11Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == personOperationFactoriesCount) ? 0 : random.nextInt(0, 2);
                        return personOperationFactories[operationFactoryIndex].create(
                                typedResult.get(randomResultIndex).personId(),
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery12.TYPE: {
                    if (0 == personOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcQuery12Result> typedResult = (List<LdbcQuery12Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == personOperationFactoriesCount) ? 0 : random.nextInt(0, 2);
                        return personOperationFactories[operationFactoryIndex].create(
                                typedResult.get(randomResultIndex).personId(),
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcQuery14.TYPE: {
                    if (0 == personOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcQuery14Result> typedResult = (List<LdbcQuery14Result>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == personOperationFactoriesCount) ? 0 : random.nextInt(0, 2);
                        List<? extends Number> personIdsPath = ImmutableList.copyOf(typedResult.get(randomResultIndex).personsIdsInPath());
                        int pathIndex = random.nextInt(0, personIdsPath.size() - 1);
                        long personId = personIdsPath.get(pathIndex).longValue();
                        return personOperationFactories[operationFactoryIndex].create(
                                personId,
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcShortQuery2PersonPosts.TYPE: {
                    if (0 == messageOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcShortQuery2PersonPostsResult> typedResult = (List<LdbcShortQuery2PersonPostsResult>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == messageOperationFactoriesCount) ? 0 : random.nextInt(0, 3);
                        return messageOperationFactories[operationFactoryIndex].create(
                                typedResult.get(randomResultIndex).messageId(),
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcShortQuery3PersonFriends.TYPE: {
                    if (0 == personOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcShortQuery3PersonFriendsResult> typedResult = (List<LdbcShortQuery3PersonFriendsResult>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == personOperationFactoriesCount) ? 0 : random.nextInt(0, 2);
                        return personOperationFactories[operationFactoryIndex].create(
                                typedResult.get(randomResultIndex).personId(),
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable typedResult
                        return null;
                    }
                }
                case LdbcShortQuery5MessageCreator.TYPE: {
                    if (0 == personOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    LdbcShortQuery5MessageCreatorResult typedResult = (LdbcShortQuery5MessageCreatorResult) result;
                    int operationFactoryIndex = (1 == personOperationFactoriesCount) ? 0 : random.nextInt(0, 2);
                    return personOperationFactories[operationFactoryIndex].create(
                            typedResult.personId(),
                            operation.scheduledStartTimeAsMilli()
                    );
                }
                case LdbcShortQuery7MessageReplies.TYPE: {
                    if (0 == messageOperationFactoriesCount) {
                        // end of random walk, all person short queries are disabled
                        return null;
                    }
                    List<LdbcShortQuery7MessageRepliesResult> typedResult = (List<LdbcShortQuery7MessageRepliesResult>) result;
                    int resultCount = typedResult.size();
                    if (resultCount > 0) {
                        int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                        int operationFactoryIndex = (1 == messageOperationFactoriesCount) ? 0 : random.nextInt(0, 3);
                        return messageOperationFactories[operationFactoryIndex].create(
                                typedResult.get(randomResultIndex).commentId(),
                                operation.scheduledStartTimeAsMilli()
                        );
                    } else {
                        // end of random walk, previous step returned no usable result
                        return null;
                    }
                }
                default:
                    return null;
            }
        } else {
            // operation result not usable for random walk OR state is lower than minimum threshold, i.e., random walk has completed
            return null;
        }
    }

    @Override
    public double updateState(double state) {
        return state * probabilityDegradationFactor;
    }

    private interface LdbcShortQueryFactory {
        Operation create(long id, long previousScheduledStartTime);
    }

    private class LdbcShortQuery1Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id, long previousScheduledStartTime) {
            Operation operation = new LdbcShortQuery1PersonProfile(id);
            operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
            return operation;
        }
    }

    private class LdbcShortQuery2Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id, long previousScheduledStartTime) {
            Operation operation = new LdbcShortQuery2PersonPosts(id, LdbcShortQuery2PersonPosts.DEFAULT_LIMIT);
            operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
            return operation;
        }
    }

    private class LdbcShortQuery3Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id, long previousScheduledStartTime) {
            Operation operation = new LdbcShortQuery3PersonFriends(id);
            operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
            return operation;
        }
    }

    private class LdbcShortQuery4Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id, long previousScheduledStartTime) {
            Operation operation = new LdbcShortQuery4MessageContent(id);
            operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
            return operation;
        }
    }

    private class LdbcShortQuery5Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id, long previousScheduledStartTime) {
            Operation operation = new LdbcShortQuery5MessageCreator(id);
            operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
            return operation;
        }
    }

    private class LdbcShortQuery6Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id, long previousScheduledStartTime) {
            Operation operation = new LdbcShortQuery6MessageForum(id);
            operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
            return operation;
        }
    }

    private class LdbcShortQuery7Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id, long previousScheduledStartTime) {
            Operation operation = new LdbcShortQuery7MessageReplies(id);
            operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
            return operation;
        }
    }
}