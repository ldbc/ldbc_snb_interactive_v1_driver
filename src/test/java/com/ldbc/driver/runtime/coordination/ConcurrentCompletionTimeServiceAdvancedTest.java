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
import com.ldbc.driver.runtime.QueueEventFetcher;
import com.ldbc.driver.runtime.QueueEventSubmitter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.simple.SimpleWorkload;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class ConcurrentCompletionTimeServiceAdvancedTest {
    final TimeSource TIME_SOURCE = new SystemTimeSource();
    final CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();
    final Integer TERMINATE = -1;

    @Ignore
    @Test
    public void comparePerformanceOfQueueImplementationsDuringConcurrentAccess() throws InterruptedException {
        int queueItemCount = 1000000;
        int experimentCount = 5;

        Duration totalDurationConcurrentLinkedQueueNonBlocking = Duration.fromMilli(0);

        Duration totalDurationLinkedBlockingQueueNonBlocking = Duration.fromMilli(0);
        Duration totalDurationLinkedBlockingQueueBlocking = Duration.fromMilli(0);

        Duration totalDurationArrayBlockingQueueNonBlocking = Duration.fromMilli(0);
        Duration totalDurationArrayBlockingQueueBlocking = Duration.fromMilli(0);

        Duration totalDurationLinkedTransferQueueAddPollNonBlocking = Duration.fromMilli(0);
        Duration totalDurationLinkedTransferQueuePutTakeBlocking = Duration.fromMilli(0);

        Duration totalDurationSynchronousQueueBlocking = Duration.fromMilli(0);

        for (int i = 0; i < experimentCount; i++) {
            totalDurationConcurrentLinkedQueueNonBlocking =
                    totalDurationConcurrentLinkedQueueNonBlocking.plus(nonBlockingQueuePerformanceTest(queueItemCount, new ConcurrentLinkedQueue<Integer>()));

            totalDurationLinkedBlockingQueueNonBlocking =
                    totalDurationLinkedBlockingQueueNonBlocking.plus(nonBlockingQueuePerformanceTest(queueItemCount, new LinkedBlockingQueue<Integer>()));
            totalDurationLinkedBlockingQueueBlocking =
                    totalDurationLinkedBlockingQueueBlocking.plus(blockingQueuePerformanceTest(queueItemCount, new LinkedBlockingQueue<Integer>()));

            totalDurationArrayBlockingQueueNonBlocking =
                    totalDurationArrayBlockingQueueNonBlocking.plus(nonBlockingQueuePerformanceTest(queueItemCount, new ArrayBlockingQueue<Integer>(queueItemCount)));
            totalDurationArrayBlockingQueueBlocking =
                    totalDurationArrayBlockingQueueBlocking.plus(blockingQueuePerformanceTest(queueItemCount, new ArrayBlockingQueue<Integer>(queueItemCount)));

            totalDurationLinkedTransferQueueAddPollNonBlocking =
                    totalDurationLinkedTransferQueueAddPollNonBlocking.plus(nonBlockingQueuePerformanceTest(queueItemCount, new LinkedTransferQueue<Integer>()));
            totalDurationLinkedTransferQueuePutTakeBlocking =
                    totalDurationLinkedTransferQueuePutTakeBlocking.plus(blockingQueuePerformanceTest(queueItemCount, new LinkedTransferQueue<Integer>()));

            totalDurationSynchronousQueueBlocking =
                    totalDurationSynchronousQueueBlocking.plus(blockingQueuePerformanceTest(queueItemCount, new SynchronousQueue<Integer>()));
        }

        long concurrentLinkedQueueNonBlockingItemsPerMs = (queueItemCount * experimentCount) / totalDurationConcurrentLinkedQueueNonBlocking.asMilli();

        long linkedBlockingQueueNonBlockingItemsPerMs = (queueItemCount * experimentCount) / totalDurationLinkedBlockingQueueNonBlocking.asMilli();
        long linkedBlockingQueueBlockingItemsPerMs = (queueItemCount * experimentCount) / totalDurationLinkedBlockingQueueBlocking.asMilli();

        long arrayBlockingQueueNonBlockingItemsPerMs = (queueItemCount * experimentCount) / totalDurationArrayBlockingQueueNonBlocking.asMilli();
        long arrayBlockingQueueBlockingItemsPerMs = (queueItemCount * experimentCount) / totalDurationArrayBlockingQueueBlocking.asMilli();

        long linkedTransferQueueNonBlockingItemsPerMs = (queueItemCount * experimentCount) / totalDurationLinkedTransferQueueAddPollNonBlocking.asMilli();
        long linkedTransferQueueBlockingItemsPerMs = (queueItemCount * experimentCount) / totalDurationLinkedTransferQueuePutTakeBlocking.asMilli();

        long synchronousQueueBlockingItemsPerMs = (queueItemCount * experimentCount) / totalDurationSynchronousQueueBlocking.asMilli();


        System.out.println("ConcurrentLinkedQueue(non-blocking) = \t" + concurrentLinkedQueueNonBlockingItemsPerMs + " item/ms");

        System.out.println("LinkedBlockingQueue(non-blocking) = \t" + linkedBlockingQueueNonBlockingItemsPerMs + " item/ms");
        System.out.println("LinkedBlockingQueue(blocking) = \t\t" + linkedBlockingQueueBlockingItemsPerMs + " item/ms");

        System.out.println("ArrayBlockingQueue(non-blocking) = \t\t" + arrayBlockingQueueNonBlockingItemsPerMs + " item/ms");
        System.out.println("ArrayBlockingQueue(blocking) = \t\t\t" + arrayBlockingQueueBlockingItemsPerMs + " item/ms");

        System.out.println("LinkedTransferQueue(non-blocking) = \t" + linkedTransferQueueNonBlockingItemsPerMs + " item/ms");
        System.out.println("LinkedTransferQueue(blocking) = \t\t" + linkedTransferQueueBlockingItemsPerMs + " item/ms");

        System.out.println("SynchronousQueue(blocking) = \t\t\t" + synchronousQueueBlockingItemsPerMs + " item/ms");
    }

    public Duration nonBlockingQueuePerformanceTest(final int queueItemCount, final Queue<Integer> queue) throws InterruptedException {
        Thread writeThread = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < queueItemCount; i++) {
                    queue.add(i);
                }
                queue.add(TERMINATE);
            }
        };

        Thread readThread = new Thread() {
            @Override
            public void run() {
                Integer val = 0;
                while (TERMINATE.equals(val) == false) {
                    val = queue.poll();
                }
            }
        };

        long startTimeAsMilli = TIME_SOURCE.nowAsMilli();
        readThread.start();
        writeThread.start();
        writeThread.join();
        readThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return Duration.fromMilli(TIME_SOURCE.nowAsMilli() - startTimeAsMilli);
    }

    public Duration blockingQueuePerformanceTest(final int queueItemCount, final BlockingQueue<Integer> queue) throws InterruptedException {
        Thread writeThread = new Thread() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < queueItemCount; i++) {
                        queue.put(i);
                    }
                    queue.put(TERMINATE);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        Thread readThread = new Thread() {
            @Override
            public void run() {
                try {
                    Integer val = 0;
                    while (TERMINATE.equals(val) == false) {
                        val = queue.take();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        long startTimeAsMilli = TIME_SOURCE.nowAsMilli();
        readThread.start();
        writeThread.start();
        writeThread.join();
        readThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return Duration.fromMilli(TIME_SOURCE.nowAsMilli() - startTimeAsMilli);
    }

    @Ignore
    @Test
    public void completionTimeServicesShouldBehaveDeterministically() throws InterruptedException, ExecutionException, WorkloadException, CompletionTimeException, DriverConfigurationException {
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
                        completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(TIME_SOURCE, peerIds, errorReporter);
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
                                                      int workerThreadCount)
            throws WorkloadException, InterruptedException, ExecutionException, CompletionTimeException, DriverConfigurationException {
        // initialize executor
        int threadCount = workerThreadCount;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<Integer> completionService = new ExecutorCompletionService<>(executorService);

        // initialize workload
        int operationCountCheckPoint1 = 100;
        int operationCountCheckPoint2 = 900;
        long operationCountAdditionalOperations = 100000;
        long operationCount = operationCountCheckPoint1 + operationCountCheckPoint2 + operationCountAdditionalOperations;

        LocalCompletionTimeWriter localCompletionTimeWriter = concurrentCompletionTimeService.newLocalCompletionTimeWriter();
        ExternalCompletionTimeWriter externalCompletionTimeWriter = concurrentCompletionTimeService;
        GlobalCompletionTimeReader globalCompletionTimeReader = concurrentCompletionTimeService;

        String databaseClassName = null;
        String workloadClassName = null;
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults(databaseClassName, workloadClassName, operationCount);

        // TODO consider using DummyWorkload instead
        Workload workload = new SimpleWorkload();
        workload.init(configuration);
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.operations(generators, configuration.operationCount());

        // measure duration of experiment
        Time startTime = TIME_SOURCE.now();

        // track number of operations that have completed, i.e., that have their Completed Time submitted
        int completedOperations = 0;

        /*
        CREATE 1st CHECK POINT
         */
        Operation<?> gctCheckpointOperation1 = operations.next();
        Time gctCheckpointOperation1ScheduledStartTime = gctCheckpointOperation1.scheduledStartTime();
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, gctCheckpointOperation1ScheduledStartTime);
        localCompletionTimeWriter.submitLocalInitiatedTime(gctCheckpointOperation1ScheduledStartTime);
        localCompletionTimeWriter.submitLocalCompletedTime(gctCheckpointOperation1ScheduledStartTime);
        completedOperations++;

        // This is only used for ensuring that time stamps are in fact monotonically increasing
        Time lastScheduledStartTime = gctCheckpointOperation1ScheduledStartTime;

        for (int i = completedOperations; i < operationCountCheckPoint1; i++) {
            Operation<?> operation = operations.next();
            assertThat(operation.scheduledStartTime().gte(lastScheduledStartTime), is(true));
            lastScheduledStartTime = operation.scheduledStartTime();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.scheduledStartTime());
            completionService.submit(new GctAccessingCallable(operation, localCompletionTimeWriter, globalCompletionTimeReader, errorReporter));
        }

        // Wait for tasks to finish submitting Completed Times, up to 1st check point
        while (completedOperations < operationCountCheckPoint1) {
            completionService.take();
            completedOperations++;
        }

        /*
        TEST 1st CHECK POINT
         */
        Future<Time> future1WaitingForGtcCheckpointOperation1 = concurrentCompletionTimeService.globalCompletionTimeFuture();
        assertThat(future1WaitingForGtcCheckpointOperation1.get(), equalTo(gctCheckpointOperation1ScheduledStartTime));

        /*
        CREATE 2nd CHECK POINT
         */
        Operation<?> gctCheckpointOperation2 = operations.next();
        Time gctCheckpointOperation2ScheduledStartTime = gctCheckpointOperation2.scheduledStartTime();
        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, gctCheckpointOperation2ScheduledStartTime);
        localCompletionTimeWriter.submitLocalInitiatedTime(gctCheckpointOperation2ScheduledStartTime);
        localCompletionTimeWriter.submitLocalCompletedTime(gctCheckpointOperation2ScheduledStartTime);
        completedOperations++;

        for (int i = completedOperations; i < operationCountCheckPoint2; i++) {
            Operation<?> operation = operations.next();
            assertThat(operation.scheduledStartTime().gte(lastScheduledStartTime), is(true));
            lastScheduledStartTime = operation.scheduledStartTime();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.scheduledStartTime());
            completionService.submit(new GctAccessingCallable(operation, localCompletionTimeWriter, globalCompletionTimeReader, errorReporter));
        }

        // Wait for tasks to finish submitting Completed Times, up to 2nd check point
        while (completedOperations < operationCountCheckPoint2) {
            completionService.take();
            completedOperations++;
        }

        /*
        TEST 2nd CHECK POINT
         */
        Future<Time> future2WaitingForGtcCheckpointOperation2 = concurrentCompletionTimeService.globalCompletionTimeFuture();
        assertThat(future2WaitingForGtcCheckpointOperation2.get(), equalTo(gctCheckpointOperation2ScheduledStartTime));

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            assertThat(operation.scheduledStartTime().gte(lastScheduledStartTime), is(true));
            lastScheduledStartTime = operation.scheduledStartTime();
            localCompletionTimeWriter.submitLocalInitiatedTime(operation.scheduledStartTime());
            completionService.submit(new GctAccessingCallable(operation, localCompletionTimeWriter, globalCompletionTimeReader, errorReporter));
        }

        // Wait for tasks to finish submitting Completed Times, up to final check point (end of workload)
        while (completedOperations < operationCount) {
            completionService.take();
            completedOperations++;
        }

        // Submit one more local initiated time to allow local completion time to advance to the last submitted completed time
        Time slightlyAfterLastScheduledStartTime = lastScheduledStartTime.plus(Duration.fromNano(1));
        localCompletionTimeWriter.submitLocalInitiatedTime(slightlyAfterLastScheduledStartTime);

        externalCompletionTimeWriter.submitPeerCompletionTime(otherPeerId, lastScheduledStartTime);

        /*
        TEST 3rd CHECK POINT
         */
        Future<Time> future3WaitingForLastOperation = concurrentCompletionTimeService.globalCompletionTimeFuture();
        assertThat(future3WaitingForLastOperation.get(), equalTo(lastScheduledStartTime));

        Duration testDuration = TIME_SOURCE.now().durationGreaterThan(startTime);

        executorService.shutdown();
        boolean allTasksCompletedInTime = executorService.awaitTermination(1, TimeUnit.SECONDS);
        assertThat(allTasksCompletedInTime, is(true));
        assertThat(errorReporter.errorEncountered(), is(false));
        workload.cleanup();
        return testDuration;
    }

    class GctAccessingCallable implements Callable<Integer> {
        private final Operation<?> operation;
        private final LocalCompletionTimeWriter localCompletionTimeWriter;
        private final GlobalCompletionTimeReader globalCompletionTimeReader;
        private final ConcurrentErrorReporter errorReporter;

        public GctAccessingCallable(Operation<?> operation,
                                    LocalCompletionTimeWriter localCompletionTimeWriter,
                                    GlobalCompletionTimeReader globalCompletionTimeReader,
                                    ConcurrentErrorReporter errorReporter) {
            this.operation = operation;
            this.localCompletionTimeWriter = localCompletionTimeWriter;
            this.globalCompletionTimeReader = globalCompletionTimeReader;
            this.errorReporter = errorReporter;
        }

        public Integer call() throws Exception {
            try {
                Time gctTime = globalCompletionTimeReader.globalCompletionTime();
                assertThat(gctTime, notNullValue());
                // operation completes
                localCompletionTimeWriter.submitLocalCompletedTime(operation.scheduledStartTime());
                return 1;
            } catch (Exception e) {
                errorReporter.reportError(this, "Error in call()");
                return -1;
            }
        }
    }

}
