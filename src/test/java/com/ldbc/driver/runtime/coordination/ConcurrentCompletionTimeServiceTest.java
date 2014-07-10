package com.ldbc.driver.runtime.coordination;

import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.simple.SimpleWorkload;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class ConcurrentCompletionTimeServiceTest {
    final TimeSource TIME_SOURCE = new SystemTimeSource();
    final Integer TERMINATE = -1;
    final int QUEUE_ITEM_COUNT = 10000000;

    @Test
    public void comparePerformanceOfQueueImplementationsDuringConcurrentAccess() throws InterruptedException {
        Duration concurrentLinkedQueueAddPollDuration = threadedConcurrentLinkedQueueAddPollPerformanceTest();
        Duration linkedBlockingQueueAddPollDuration = threadedLinkedBlockingQueueAddPollPerformanceTest();
        Duration linkedBlockingQueuePutTakeDuration = threadedLinkedBlockingQueuePutTakePerformanceTest();
        System.out.println("ConcurrentLinkedQueue(add/poll) = \t" + concurrentLinkedQueueAddPollDuration.toString());
        System.out.println("LinkedBlockingQueue(add/poll) = \t" + linkedBlockingQueueAddPollDuration.toString());
        System.out.println("LinkedBlockingQueue(put/take) = \t" + linkedBlockingQueuePutTakeDuration.toString());
    }

    public Duration threadedConcurrentLinkedQueueAddPollPerformanceTest() throws InterruptedException {
        final Queue<Integer> queue = new ConcurrentLinkedQueue<>();

        Thread writeThread = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < QUEUE_ITEM_COUNT; i++) {
                    queue.add(i);
                }
                queue.add(-1);
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
        readThread.join();
        writeThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return Duration.fromMilli(TIME_SOURCE.nowAsMilli() - startTimeAsMilli);
    }

    public Duration threadedLinkedBlockingQueueAddPollPerformanceTest() throws InterruptedException {
        final BlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();

        Thread writeThread = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < QUEUE_ITEM_COUNT; i++) {
                    queue.add(i);
                }
                queue.add(-1);
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
        readThread.join();
        writeThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return Duration.fromMilli(TIME_SOURCE.nowAsMilli() - startTimeAsMilli);
    }

    public Duration threadedLinkedBlockingQueuePutTakePerformanceTest() throws InterruptedException {
        final BlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();

        Thread writeThread = new Thread() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < QUEUE_ITEM_COUNT; i++) {
                        queue.put(i);
                    }
                    queue.put(-1);
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
        readThread.join();
        writeThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return Duration.fromMilli(TIME_SOURCE.nowAsMilli() - startTimeAsMilli);
    }

    @Test
    public void completionTimeServicesShouldBehaveDeterministically() throws InterruptedException, ExecutionException, WorkloadException, CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        String otherPeerId = "somePeer";
        Set<String> peerIds = Sets.newHashSet(otherPeerId);
        int testRepetitions = 5;
        long syncDuration;
        long threadedDuration;

        for (int workerThreads = 1; workerThreads < 33; workerThreads = workerThreads * 2) {
            syncDuration = 0;
            for (int i = 0; i < testRepetitions; i++) {
                ConcurrentCompletionTimeService concurrentCompletionTimeService = new NaiveSynchronizedConcurrentCompletionTimeService(peerIds);
                syncDuration += coordinationCompletionTimeServiceTest(concurrentCompletionTimeService, otherPeerId, errorReporter, workerThreads).asMilli();
                concurrentCompletionTimeService.shutdown();
            }
            System.out.printf("Threads:%-2s\t%s=%s",
                    workerThreads,
                    NaiveSynchronizedConcurrentCompletionTimeService.class.getSimpleName(),
                    Duration.fromMilli(syncDuration / testRepetitions).toString());
            threadedDuration = 0;
            for (int i = 0; i < testRepetitions; i++) {
                ConcurrentCompletionTimeService concurrentCompletionTimeService = new ThreadedQueuedConcurrentCompletionTimeService(TIME_SOURCE, peerIds, errorReporter);
                threadedDuration += coordinationCompletionTimeServiceTest(concurrentCompletionTimeService, otherPeerId, errorReporter, workerThreads).asMilli();
                concurrentCompletionTimeService.shutdown();
            }
            System.out.printf("\t%s=%s\n",
                    ThreadedQueuedConcurrentCompletionTimeService.class.getSimpleName(),
                    Duration.fromMilli(threadedDuration / testRepetitions).toString());
        }
    }

    public Duration coordinationCompletionTimeServiceTest(ConcurrentCompletionTimeService completionTimeService,
                                                          String otherPeerId,
                                                          ConcurrentErrorReporter errorReporter,
                                                          int workerThreadCount)
            throws WorkloadException, InterruptedException, ExecutionException, CompletionTimeException {
        // initialize executor
        int threadCount = workerThreadCount;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<Integer> executor = new ExecutorCompletionService<Integer>(executorService);

        // initialize workload
        long operationCount = 100000;
        int operationCountCheckPoint1 = 100;
        int operationCountCheckPoint2 = 900;
        // to make sure it's a valid test
        assertThat(operationCount > operationCountCheckPoint1 + operationCountCheckPoint2, is(true));

        Map<String, String> paramsMap = null;
        String className = null;
        String workloadName = null;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = null;
        String resultFilePath = "nothingPath";
        double timeCompressionRatio = 1.0;
        Duration gctDeltaDuration = null;
        Duration windowedExecutionWindowDuration = null;
        Set<String> peerIds = null;
        Duration toleratedDelay = null;
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;

        ConsoleAndFileDriverConfiguration params =
                new ConsoleAndFileDriverConfiguration(paramsMap, className, workloadName, operationCount, threadCount, statusDisplayInterval, timeUnit,
                        resultFilePath, timeCompressionRatio, gctDeltaDuration, windowedExecutionWindowDuration, peerIds, toleratedDelay,
                        validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration, printHelp);
        Workload workload = new SimpleWorkload();
        workload.init(params);
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.operations(generators, params.operationCount());

        // measure duration of experiment
        long startTimeAsMilli = TIME_SOURCE.nowAsMilli();

        Operation<?> gctCheckpointOperation1 = operations.next();
        completionTimeService.submitExternalCompletionTime(otherPeerId, gctCheckpointOperation1.scheduledStartTime());
        completionTimeService.submitInitiatedTime(gctCheckpointOperation1.scheduledStartTime());
        completionTimeService.submitCompletedTime(gctCheckpointOperation1.scheduledStartTime());

        Time lastScheduledStartTime = gctCheckpointOperation1.scheduledStartTime();

        for (int i = 1; i < operationCountCheckPoint1; i++) {
            Operation<?> operation = operations.next();
            // Just to make sure that time stamps are in fact increasing
            assertThat(operation.scheduledStartTime().asMilli() >= lastScheduledStartTime.asMilli(), is(true));
            lastScheduledStartTime = operation.scheduledStartTime();
            // IMPORTANT: queue initiated event BEFORE submitting task
            // This ensures that the event is initiated before later events initiate AND complete
            // Otherwise race conditions can cause GCT to proceed before an earlier initiated time is logged
            completionTimeService.submitInitiatedTime(operation.scheduledStartTime());
            executor.submit(new GctAccessingCallable(operation, completionTimeService, errorReporter));
        }

        int completedTasks = 1;
        while (completedTasks < operationCountCheckPoint1) {
            executor.take();
            completedTasks++;
        }

        Future<Time> gctFuture1 = completionTimeService.globalCompletionTimeFuture();
        assertThat(gctFuture1.get(), equalTo(gctCheckpointOperation1.scheduledStartTime()));

        Operation<?> gctCheckpointOperation2 = operations.next();
        completionTimeService.submitExternalCompletionTime(otherPeerId, gctCheckpointOperation2.scheduledStartTime());
        completionTimeService.submitInitiatedTime(gctCheckpointOperation2.scheduledStartTime());
        executor.submit(new GctAccessingCallable(gctCheckpointOperation2, completionTimeService, errorReporter));

        for (int i = operationCountCheckPoint1 + 1; i < operationCountCheckPoint2; i++) {
            Operation<?> operation = operations.next();
            assertThat(operation.scheduledStartTime().asMilli() >= lastScheduledStartTime.asMilli(), is(true));
            lastScheduledStartTime = operation.scheduledStartTime();
            completionTimeService.submitInitiatedTime(operation.scheduledStartTime());
            executor.submit(new GctAccessingCallable(operation, completionTimeService, errorReporter));
        }

        // Wait for tasks to finish submitting CompletedTimes
        completedTasks = 0;
        while (completedTasks < operationCountCheckPoint2 - operationCountCheckPoint1) {
            executor.take();
            completedTasks++;
        }

        Future<Time> gctFuture2 = completionTimeService.globalCompletionTimeFuture();
        assertThat(gctFuture2.get(), equalTo(gctCheckpointOperation2.scheduledStartTime()));

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            // Just to make sure that time stamps are in fact increasing
            assertThat(operation.scheduledStartTime().gt(lastScheduledStartTime), is(true));
            lastScheduledStartTime = operation.scheduledStartTime();
            // TODO document
            // IMPORTANT: queue initiated event BEFORE submitting task
            // This ensures that the event is initiated before later events initiate AND complete
            // Otherwise race conditions can cause GCT to proceed before an earlier initiated time is logged
            completionTimeService.submitInitiatedTime(operation.scheduledStartTime());
            executor.submit(new GctAccessingCallable(operation, completionTimeService, errorReporter));
        }

        // Wait for tasks to finish submitting CompletedTimes
        completedTasks = 0;
        while (completedTasks < operationCount - operationCountCheckPoint2) {
            executor.take();
            completedTasks++;
        }

        completionTimeService.submitExternalCompletionTime(otherPeerId, lastScheduledStartTime);

        Future<Time> gctFuture3 = completionTimeService.globalCompletionTimeFuture();
        assertThat(gctFuture3.get(), equalTo(lastScheduledStartTime));

        Duration testDuration = Duration.fromMilli(TIME_SOURCE.nowAsMilli() - startTimeAsMilli);

        executorService.shutdown();
        boolean allTasksCompletedInTime = executorService.awaitTermination(1, TimeUnit.SECONDS);
        assertThat(allTasksCompletedInTime, is(true));
        assertThat(errorReporter.errorEncountered(), is(false));
        workload.cleanup();
        return testDuration;
    }

    class GctAccessingCallable implements Callable<Integer> {
        private final Operation<?> operation;
        private final ConcurrentCompletionTimeService concurrentCompletionTimeService;
        private final ConcurrentErrorReporter errorReporter;

        public GctAccessingCallable(Operation<?> operation,
                                    ConcurrentCompletionTimeService concurrentCompletionTimeService,
                                    ConcurrentErrorReporter errorReporter) {
            this.operation = operation;
            this.concurrentCompletionTimeService = concurrentCompletionTimeService;
            this.errorReporter = errorReporter;
        }

        public Integer call() throws Exception {
            try {
                Time gctTime = concurrentCompletionTimeService.globalCompletionTime();
                assertThat(gctTime, notNullValue());
                // operation completes
                // TODO in real application this should be done AFTER completion of operation
                // TODO write lessons learnt into Confluence specification document
                concurrentCompletionTimeService.submitCompletedTime(operation.scheduledStartTime());
                return 1;
            } catch (Exception e) {
                errorReporter.reportError(this, "Error in call()");
                return -1;
            }
        }
    }

}
