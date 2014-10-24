package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
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

public class ConcurrentCompletionTimeServiceAdvancedTest {
    final TimeSource timeSource = new SystemTimeSource();
    final CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

    @Ignore
    @Test
    public void stressTestThreadedQueuedConcurrentCompletionTimeService() throws InterruptedException, ExecutionException, WorkloadException, CompletionTimeException, DriverConfigurationException, IOException {
        ThreadPoolLoadGenerator threadPoolLoadGenerator = TestUtils.newThreadPoolLoadGenerator(128, Duration.fromMilli(0));
        threadPoolLoadGenerator.start();
        try {
            int testRepetitions = 10;

            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            String otherPeerId = "somePeer";
            Set<String> peerIds = Sets.newHashSet(otherPeerId);
            long totalTestDurationForThreadedCompletionTimeService;

            for (int workerThreads = 1; workerThreads < 33; workerThreads = workerThreads * 2) {
                totalTestDurationForThreadedCompletionTimeService = 0;
                for (int i = 0; i < testRepetitions; i++) {
                    ConcurrentCompletionTimeService concurrentCompletionTimeService =
                            completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);
                    try {
                        totalTestDurationForThreadedCompletionTimeService += parallelCompletionTimeServiceTest(concurrentCompletionTimeService, otherPeerId, errorReporter, workerThreads).asMilli();
                    } finally {
                        concurrentCompletionTimeService.shutdown();
                    }
                }
                System.out.printf("\t%s=%s\n",
                        ThreadedQueuedConcurrentCompletionTimeService.class.getSimpleName(),
                        Duration.fromMilli(totalTestDurationForThreadedCompletionTimeService / testRepetitions).toString());
            }
        } finally {
            threadPoolLoadGenerator.shutdown(Duration.fromSeconds(10));
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
                ConcurrentCompletionTimeService concurrentConcurrentCompletionTimeService =
                        completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);
                totalTestDurationForSynchronousCompletionTimeService += parallelCompletionTimeServiceTest(concurrentConcurrentCompletionTimeService, otherPeerId, errorReporter, workerThreads).asMilli();
                concurrentConcurrentCompletionTimeService.shutdown();
            }
            System.out.printf("Threads:%-2s\t%s=%s",
                    workerThreads,
                    SynchronizedConcurrentCompletionTimeService.class.getSimpleName(),
                    Duration.fromMilli(totalTestDurationForSynchronousCompletionTimeService / testRepetitions).toString());

            totalTestDurationForThreadedCompletionTimeService = 0;
            for (int i = 0; i < testRepetitions; i++) {
                ConcurrentCompletionTimeService concurrentCompletionTimeService =
                        completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource, peerIds, errorReporter);
                totalTestDurationForThreadedCompletionTimeService += parallelCompletionTimeServiceTest(concurrentCompletionTimeService, otherPeerId, errorReporter, workerThreads).asMilli();
                concurrentCompletionTimeService.shutdown();
            }
            System.out.printf("\t%s=%s\n",
                    ThreadedQueuedConcurrentCompletionTimeService.class.getSimpleName(),
                    Duration.fromMilli(totalTestDurationForThreadedCompletionTimeService / testRepetitions).toString());
        }
    }

    public Duration parallelCompletionTimeServiceTest(ConcurrentCompletionTimeService concurrentCompletionTimeService,
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
                        ConcurrentCompletionTimeServiceAdvancedTest.class.getSimpleName() + ".parallelCompletionTimeServiceTest-id(" + factoryTimeStampId + ")" + "-thread(" + count++ + ")");
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

        LocalCompletionTimeWriter localCompletionTimeWriter = concurrentCompletionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = concurrentCompletionTimeService;

        String databaseClassName = null;
        String workloadClassName = null;
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults(databaseClassName, workloadClassName, operationCount);

        // TODO consider using DummyWorkload instead
        Workload workload = new SimpleWorkload();
        workload.init(configuration);
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = gf.limit(workload.streams(gf).mergeSortedByStartTime(gf), configuration.operationCount());

        // measure duration of experiment
        Time startTime = timeSource.now();

        // track number of operations that have completed, i.e., that have their Completed Time submitted
        int completedOperations = 0;

        /*
        CREATE 1st CHECK POINT
         */
        Operation<?> gctCheckpointOperation1 = operations.next();
        Time gctCheckpointOperation1ScheduledStartTime = gctCheckpointOperation1.scheduledStartTimeAsMilli();
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, gctCheckpointOperation1ScheduledStartTime);
        localCompletionTimeWriter.submitLocalInitiatedTime(gctCheckpointOperation1ScheduledStartTime);
        localCompletionTimeWriter.submitLocalCompletedTime(gctCheckpointOperation1ScheduledStartTime);
        completedOperations++;

        // This is only used for ensuring that time stamps are in fact monotonically increasing
        Time lastScheduledStartTime = gctCheckpointOperation1ScheduledStartTime;

        for (int i = completedOperations; i < operationCountCheckPoint1; i++) {
            Operation<?> operation = operations.next();
            assertThat(operation.scheduledStartTimeAsMilli().gte(lastScheduledStartTime), is(true));
            lastScheduledStartTime = operation.scheduledStartTimeAsMilli();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.scheduledStartTimeAsMilli());
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
        Future<Time> future1WaitingForGtcCheckpointOperation1 = concurrentCompletionTimeService.globalCompletionTimeFuture();
        assertThat(future1WaitingForGtcCheckpointOperation1.get(), equalTo(gctCheckpointOperation1ScheduledStartTime));

        /*
        CREATE 2nd CHECK POINT
         */
        Operation<?> gctCheckpointOperation2 = operations.next();
        Time gctCheckpointOperation2ScheduledStartTime = gctCheckpointOperation2.scheduledStartTimeAsMilli();
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, gctCheckpointOperation2ScheduledStartTime);
        localCompletionTimeWriter.submitLocalInitiatedTime(gctCheckpointOperation2ScheduledStartTime);
        localCompletionTimeWriter.submitLocalCompletedTime(gctCheckpointOperation2ScheduledStartTime);
        completedOperations++;

        for (int i = completedOperations; i < operationCountCheckPoint2; i++) {
            Operation<?> operation = operations.next();
            assertThat(operation.scheduledStartTimeAsMilli().gte(lastScheduledStartTime), is(true));
            lastScheduledStartTime = operation.scheduledStartTimeAsMilli();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.scheduledStartTimeAsMilli());
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
        Future<Time> future2WaitingForGtcCheckpointOperation2 = concurrentCompletionTimeService.globalCompletionTimeFuture();
        assertThat(future2WaitingForGtcCheckpointOperation2.get(), equalTo(gctCheckpointOperation2ScheduledStartTime));

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            assertThat(operation.scheduledStartTimeAsMilli().gte(lastScheduledStartTime), is(true));
            lastScheduledStartTime = operation.scheduledStartTimeAsMilli();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.scheduledStartTimeAsMilli());
            completionService.submit(new GctAccessingCallable(operation, localCompletionTimeWriter, errorReporter));
        }

        // Wait for tasks to finish submitting Completed Times, up to final check point (end of workload)
        while (completedOperations < operationCount) {
            completionService.take();
            completedOperations++;
        }

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // Submit one more local initiated time to allow local completion time to advance to the last submitted completed time
        Time slightlyAfterLastScheduledStartTime = lastScheduledStartTime.plus(Duration.fromNano(1));
        localCompletionTimeWriter.submitLocalInitiatedTime(slightlyAfterLastScheduledStartTime);

        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, lastScheduledStartTime);

        /*
        TEST 3rd CHECK POINT
         */
        Future<Time> future3WaitingForLastOperation = concurrentCompletionTimeService.globalCompletionTimeFuture();
        assertThat(future3WaitingForLastOperation.get(), equalTo(lastScheduledStartTime));

        Duration testDuration = timeSource.now().durationGreaterThan(startTime);

        executorService.shutdown();
        boolean allTasksCompletedInTime = executorService.awaitTermination(10, TimeUnit.SECONDS);
        assertThat(allTasksCompletedInTime, is(true));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
        workload.close();
        return testDuration;
    }

    class GctAccessingCallable implements Callable<Integer> {
        private final Operation<?> operation;
        private final LocalCompletionTimeWriter localCompletionTimeWriter;
        private final ConcurrentErrorReporter errorReporter;

        public GctAccessingCallable(Operation<?> operation,
                                    LocalCompletionTimeWriter localCompletionTimeWriter,
                                    ConcurrentErrorReporter errorReporter) {
            this.operation = operation;
            this.localCompletionTimeWriter = localCompletionTimeWriter;
            this.errorReporter = errorReporter;
        }

        public Integer call() throws Exception {
            try {
                // operation completes
                localCompletionTimeWriter.submitLocalCompletedTime(operation.scheduledStartTimeAsMilli());
                return 1;
            } catch (Exception e) {
                errorReporter.reportError(this, "Error in call()");
                return -1;
            }
        }
    }

}
