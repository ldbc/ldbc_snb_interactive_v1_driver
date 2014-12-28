package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.List;
import java.util.Set;

public class LdbcSnbShortReadGenerator implements ChildOperationGenerator {
    private final double initialProbability;
    private final double probabilityDegradationFactor;
    private final double minimumProbability;
    private final LdbcShortQueryFactory[] personOperationFactories;
    private final LdbcShortQueryFactory[] messageOperationFactories;
    private final RandomDataGenerator random;
    private final Set<Class<? extends Operation<?>>> usableOperationTypes;

    public LdbcSnbShortReadGenerator(double initialProbability,
                                     double probabilityDegradationFactor,
                                     double minimumProbability) {
        this.initialProbability = initialProbability;
        this.probabilityDegradationFactor = probabilityDegradationFactor;
        this.minimumProbability = minimumProbability;
        this.random = new RandomDataGeneratorFactory().newRandom(42l);
        this.personOperationFactories = new LdbcShortQueryFactory[]{
                new LdbcShortQuery1Factory(),
                new LdbcShortQuery2Factory(),
                new LdbcShortQuery3Factory()
        };
        this.messageOperationFactories = new LdbcShortQueryFactory[]{
                new LdbcShortQuery4Factory(),
                new LdbcShortQuery5Factory(),
                new LdbcShortQuery6Factory(),
                new LdbcShortQuery7Factory()
        };
        this.usableOperationTypes = Sets.newHashSet(
                LdbcQuery1.class,
                LdbcQuery2.class,
                LdbcQuery3.class,
                LdbcQuery7.class,
                LdbcQuery8.class,
                LdbcQuery9.class,
                LdbcQuery10.class,
                LdbcQuery11.class,
                LdbcQuery12.class,
                LdbcQuery14.class,
                LdbcShortQuery2PersonPosts.class,
                LdbcShortQuery3PersonFriends.class,
                LdbcShortQuery5MessageCreator.class,
                LdbcShortQuery7MessageReplies.class
        );
    }

    @Override
    public double initialState() {
        return initialProbability;
    }

    @Override
    public Operation<?> nextOperation(double state, OperationResultReport resultReport) throws WorkloadException {
        if (state > minimumProbability && usableOperationTypes.contains(resultReport.operation().getClass())) {
            if (LdbcQuery1.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery1Result> results = (List<LdbcQuery1Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 2);
                    return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).friendId());
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery2.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery2Result> results = (List<LdbcQuery2Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 6);
                    if (operationFactoryIndex < 3) {
                        return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                    } else {
                        return messageOperationFactories[operationFactoryIndex - 3].create(results.get(randomResultIndex).postOrCommentId());
                    }
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery3.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery3Result> results = (List<LdbcQuery3Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 2);
                    return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery7.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery7Result> results = (List<LdbcQuery7Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 6);
                    if (operationFactoryIndex < 3) {
                        return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                    } else {
                        return messageOperationFactories[operationFactoryIndex - 3].create(results.get(randomResultIndex).commentOrPostId());
                    }
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery8.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery8Result> results = (List<LdbcQuery8Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 6);
                    if (operationFactoryIndex < 3) {
                        return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                    } else {
                        return messageOperationFactories[operationFactoryIndex - 3].create(results.get(randomResultIndex).commentId());
                    }
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery9.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery9Result> results = (List<LdbcQuery9Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 6);
                    if (operationFactoryIndex < 3) {
                        return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                    } else {
                        return messageOperationFactories[operationFactoryIndex - 3].create(results.get(randomResultIndex).commentOrPostId());
                    }
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery10.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery10Result> results = (List<LdbcQuery10Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 2);
                    return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery11.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery11Result> results = (List<LdbcQuery11Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 2);
                    return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery12.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery12Result> results = (List<LdbcQuery12Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 2);
                    return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcQuery14.class.equals(resultReport.operation().getClass())) {
                List<LdbcQuery14Result> results = (List<LdbcQuery14Result>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 2);
                    List<? extends Number> personIdsPath = ImmutableList.copyOf(results.get(randomResultIndex).personsIdsInPath());
                    int pathIndex = random.nextInt(0, personIdsPath.size() - 1);
                    long personId = personIdsPath.get(pathIndex).longValue();
                    return personOperationFactories[operationFactoryIndex].create(personId);
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcShortQuery2PersonPosts.class.equals(resultReport.operation().getClass())) {
                List<LdbcShortQuery2PersonPostsResult> results = (List<LdbcShortQuery2PersonPostsResult>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 3);
                    return messageOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).postId());
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcShortQuery3PersonFriends.class.equals(resultReport.operation().getClass())) {
                List<LdbcShortQuery3PersonFriendsResult> results = (List<LdbcShortQuery3PersonFriendsResult>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 2);
                    return personOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).personId());
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else if (LdbcShortQuery5MessageCreator.class.equals(resultReport.operation().getClass())) {
                LdbcShortQuery5MessageCreatorResult result = (LdbcShortQuery5MessageCreatorResult) resultReport.operationResult();
                int operationFactoryIndex = random.nextInt(0, 2);
                return personOperationFactories[operationFactoryIndex].create(result.personId());
            } else if (LdbcShortQuery7MessageReplies.class.equals(resultReport.operation().getClass())) {
                List<LdbcShortQuery7MessageRepliesResult> results = (List<LdbcShortQuery7MessageRepliesResult>) resultReport.operationResult();
                int resultCount = results.size();
                if (resultCount > 0) {
                    int randomResultIndex = (1 == resultCount) ? 0 : random.nextInt(0, resultCount - 1);
                    int operationFactoryIndex = random.nextInt(0, 3);
                    return messageOperationFactories[operationFactoryIndex].create(results.get(randomResultIndex).commentId());
                } else {
                    // end of random walk, previous step returned no usable results
                    return null;
                }
            } else {
                throw new WorkloadException(String.format("Invalid operation - can not be used by %s to generate child operations\n%s", getClass().getSimpleName(), resultReport.operation()));
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
        Operation create(long id);
    }

    private class LdbcShortQuery1Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id) {
            return new LdbcShortQuery1PersonProfile(id);
        }
    }

    private class LdbcShortQuery2Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id) {
            return new LdbcShortQuery2PersonPosts(id);
        }
    }

    private class LdbcShortQuery3Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id) {
            return new LdbcShortQuery3PersonFriends(id);
        }
    }

    private class LdbcShortQuery4Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id) {
            return new LdbcShortQuery4MessageContent(id);
        }
    }

    private class LdbcShortQuery5Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id) {
            return new LdbcShortQuery5MessageCreator(id);
        }
    }

    private class LdbcShortQuery6Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id) {
            return new LdbcShortQuery6MessageForum(id);
        }
    }

    private class LdbcShortQuery7Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(long id) {
            return new LdbcShortQuery7MessageReplies(id);
        }
    }
}