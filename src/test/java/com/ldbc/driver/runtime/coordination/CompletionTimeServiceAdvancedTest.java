package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.testutils.ThreadPoolLoadGenerator;
import com.ldbc.driver.workloads.simple.SimpleWorkload;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CompletionTimeServiceAdvancedTest {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    final TimeSource timeSource = new SystemTimeSource();
    final CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

    @Ignore
    @Test
    public void stressTestThreadedQueuedConcurrentCompletionTimeService() throws InterruptedException, ExecutionException, WorkloadException, CompletionTimeException, DriverConfigurationException, IOException {
        ThreadPoolLoadGenerator threadPoolLoadGenerator = TestUtils.newThreadPoolLoadGenerator(128, 0);
        threadPoolLoadGenerator.start();
        try {
            int testRepetitions = 10;

            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            String otherPeerId = "somePeer";
            Set<String> peerIds = Sets.newHashSet(otherPeerId);
            long totalTestDurationForThreadedCompletionTimeServiceAsMilli;

            for (int workerThreads = 1; workerThreads < 33; workerThreads = workerThreads * 2) {
                totalTestDurationForThreadedCompletionTimeServiceAsMilli = 0;
                for (int i = 0; i < testRepetitions; i++) {
                    CompletionTimeService completionTimeService =
                            completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);
                    try {
                        totalTestDurationForThreadedCompletionTimeServiceAsMilli += parallelCompletionTimeServiceTest(completionTimeService, otherPeerId, errorReporter, workerThreads);
                    } finally {
                        completionTimeService.shutdown();
                    }
                }
                System.out.printf("\t%s=%s\n",
                        ThreadedQueuedCompletionTimeService.class.getSimpleName(),
                        TEMPORAL_UTIL.milliDurationToString(totalTestDurationForThreadedCompletionTimeServiceAsMilli / testRepetitions));
            }
        } finally {
            threadPoolLoadGenerator.shutdown(TimeUnit.SECONDS.toMillis(10));
        }
    }

    @Test
    public void completionTimeServicesShouldBehaveDeterministically() throws InterruptedException, ExecutionException, WorkloadException, CompletionTimeException, DriverConfigurationException, IOException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        String otherPeerId = "somePeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        int testRepetitions = 5;
        long totalTestDurationForSynchronousCompletionTimeService;
        long totalTestDurationForThreadedCompletionTimeService;

        for (int workerThreads = 1; workerThreads < 33; workerThreads = workerThreads * 2) {

            totalTestDurationForSynchronousCompletionTimeService = 0;
            for (int i = 0; i < testRepetitions; i++) {
                CompletionTimeService concurrentCompletionTimeService =
                        completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);
                totalTestDurationForSynchronousCompletionTimeService += parallelCompletionTimeServiceTest(concurrentCompletionTimeService, otherPeerId, errorReporter, workerThreads);
                concurrentCompletionTimeService.shutdown();
            }
            System.out.printf("Threads:%-2s\t%s=%s",
                    workerThreads,
                    SynchronizedCompletionTimeService.class.getSimpleName(),
                    TEMPORAL_UTIL.milliDurationToString(totalTestDurationForSynchronousCompletionTimeService / testRepetitions));

            totalTestDurationForThreadedCompletionTimeService = 0;
            for (int i = 0; i < testRepetitions; i++) {
                CompletionTimeService completionTimeService =
                        completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);
                totalTestDurationForThreadedCompletionTimeService += parallelCompletionTimeServiceTest(completionTimeService, otherPeerId, errorReporter, workerThreads);
                completionTimeService.shutdown();
            }
            System.out.printf("\t%s=%s\n",
                    ThreadedQueuedCompletionTimeService.class.getSimpleName(),
                    TEMPORAL_UTIL.milliDurationToString(totalTestDurationForThreadedCompletionTimeService / testRepetitions));
        }
    }

    public long parallelCompletionTimeServiceTest(CompletionTimeService completionTimeService,
                                                  String otherPeerId,
                                                  ConcurrentErrorReporter errorReporter,
                                                  int threadCount)
            throws WorkloadException, InterruptedException, ExecutionException, CompletionTimeException, DriverConfigurationException, IOException {
        // initialize executor
        ThreadFactory threadFactory = new ThreadFactory() {
            private final long factoryTimeStampId = System.currentTimeMillis();
            int count = 0;

            @Override
            public Thread newThread(Runnable runnable) {
                Thread newThread = new Thread(
                        runnable,
                        CompletionTimeServiceAdvancedTest.class.getSimpleName() + ".parallelCompletionTimeServiceTest-id(" + factoryTimeStampId + ")" + "-thread(" + count++ + ")");
                return newThread;
            }
        };
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<Integer> completionService = new ExecutorCompletionService<>(executorService);

        // initialize workload
        int operationCountCheckPoint1 = 100;
        int operationCountCheckPoint2 = 900;
        long operationCountAdditionalOperations = 100000;
        long operationCount = operationCountCheckPoint1 + operationCountCheckPoint2 + operationCountAdditionalOperations;

        LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = completionTimeService;

        String databaseClassName = null;
        String workloadClassName = null;
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults(databaseClassName, workloadClassName, operationCount);

        // TODO consider using DummyWorkload instead
        Workload workload = new SimpleWorkload();
        workload.init(configuration);
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));

        Iterator<Operation> operations = gf.limit(
                WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf, workload.streams(gf, true)),
                configuration.operationCount()
        );

        // measure duration of experiment
        long startTimeAsMilli = timeSource.nowAsMilli();

        // track number of operations that have completed, i.e., that have their Completed Time submitted
        int completedOperations = 0;

        /*
        CREATE 1st CHECK POINT
         */
        Operation gctCheckpointOperation1 = operations.next();
        long gctCheckpointOperation1ScheduledStartTimeAsMilli = gctCheckpointOperation1.timeStamp();
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, gctCheckpointOperation1ScheduledStartTimeAsMilli);
        localCompletionTimeWriter.submitLocalInitiatedTime(gctCheckpointOperation1ScheduledStartTimeAsMilli);
        localCompletionTimeWriter.submitLocalCompletedTime(gctCheckpointOperation1ScheduledStartTimeAsMilli);
        completedOperations++;

        // This is only used for ensuring that time stamps are in fact monotonically increasing
        long lastScheduledStartTimeAsMilli = gctCheckpointOperation1ScheduledStartTimeAsMilli;

        for (int i = completedOperations; i < operationCountCheckPoint1; i++) {
            Operation operation = operations.next();
            assertThat(operation.timeStamp() >= lastScheduledStartTimeAsMilli, is(true));
            lastScheduledStartTimeAsMilli = operation.timeStamp();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.timeStamp());
            completionService.submit(new GctAccessingCallable(operation, localCompletionTimeWriter, errorReporter));
        }

        // Wait for tasks to finish submitting Completed Times, up to 1st check point
        while (completedOperations < operationCountCheckPoint1) {
            completionService.take();
            completedOperations++;
        }

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        /*
        TEST 1st CHECK POINT
         */
        Future<Long> future1WaitingForGtcCheckpointOperation1 = completionTimeService.globalCompletionTimeAsMilliFuture();
        assertThat(future1WaitingForGtcCheckpointOperation1.get(), equalTo(gctCheckpointOperation1ScheduledStartTimeAsMilli));

        /*
        CREATE 2nd CHECK POINT
         */
        Operation gctCheckpointOperation2 = operations.next();
        long gctCheckpointOperation2ScheduledStartTimeAsMilli = gctCheckpointOperation2.timeStamp();
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, gctCheckpointOperation2ScheduledStartTimeAsMilli);
        localCompletionTimeWriter.submitLocalInitiatedTime(gctCheckpointOperation2ScheduledStartTimeAsMilli);
        localCompletionTimeWriter.submitLocalCompletedTime(gctCheckpointOperation2ScheduledStartTimeAsMilli);
        completedOperations++;

        for (int i = completedOperations; i < operationCountCheckPoint2; i++) {
            Operation operation = operations.next();
            assertThat(operation.timeStamp() >= lastScheduledStartTimeAsMilli, is(true));
            lastScheduledStartTimeAsMilli = operation.timeStamp();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.timeStamp());
            completionService.submit(new GctAccessingCallable(operation, localCompletionTimeWriter, errorReporter));
        }

        // Wait for tasks to finish submitting Completed Times, up to 2nd check point
        while (completedOperations < operationCountCheckPoint2) {
            completionService.take();
            completedOperations++;
        }

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        /*
        TEST 2nd CHECK POINT
         */
        Future<Long> future2WaitingForGtcCheckpointOperation2 = completionTimeService.globalCompletionTimeAsMilliFuture();
        assertThat(future2WaitingForGtcCheckpointOperation2.get(), equalTo(gctCheckpointOperation2ScheduledStartTimeAsMilli));

        while (operations.hasNext()) {
            Operation operation = operations.next();
            assertThat(operation.timeStamp() >= lastScheduledStartTimeAsMilli, is(true));
            lastScheduledStartTimeAsMilli = operation.timeStamp();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.timeStamp());
            completionService.submit(new GctAccessingCallable(operation, localCompletionTimeWriter, errorReporter));
        }

        // Wait for tasks to finish submitting Completed Times, up to final check point (end of workload)
        while (completedOperations < operationCount) {
            completionService.take();
            completedOperations++;
        }

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // Submit one more local initiated time to allow local completion time to advance to the last submitted completed time
        long slightlyAfterLastScheduledStartTimeAsMilli = lastScheduledStartTimeAsMilli + 1;
        localCompletionTimeWriter.submitLocalInitiatedTime(slightlyAfterLastScheduledStartTimeAsMilli);

        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, lastScheduledStartTimeAsMilli);

        /*
        TEST 3rd CHECK POINT
         */
        Future<Long> future3WaitingForLastOperation = completionTimeService.globalCompletionTimeAsMilliFuture();
        assertThat(future3WaitingForLastOperation.get(), equalTo(lastScheduledStartTimeAsMilli));

        long testDurationAsMilli = timeSource.nowAsMilli() - startTimeAsMilli;

        executorService.shutdown();
        boolean allTasksCompletedInTime = executorService.awaitTermination(10, TimeUnit.SECONDS);
        assertThat(allTasksCompletedInTime, is(true));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
        workload.close();
        return testDurationAsMilli;
    }

    class GctAccessingCallable implements Callable<Integer> {
        private final Operation operation;
        private final LocalCompletionTimeWriter localCompletionTimeWriter;
        private final ConcurrentErrorReporter errorReporter;

        public GctAccessingCallable(Operation operation,
                                    LocalCompletionTimeWriter localCompletionTimeWriter,
                                    ConcurrentErrorReporter errorReporter) {
            this.operation = operation;
            this.localCompletionTimeWriter = localCompletionTimeWriter;
            this.errorReporter = errorReporter;
        }

        public Integer call() throws Exception {
            try {
                // operation completes
                localCompletionTimeWriter.submitLocalCompletedTime(operation.timeStamp());
                return 1;
            } catch (Exception e) {
                errorReporter.reportError(this, "Error in call()");
                return -1;
            }
        }
    }

}
