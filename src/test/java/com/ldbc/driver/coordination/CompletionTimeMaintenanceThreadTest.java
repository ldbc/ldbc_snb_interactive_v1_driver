package com.ldbc.driver.coordination;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadParams;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runner.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.DurationMeasurement;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.simple.SimpleWorkload;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class CompletionTimeMaintenanceThreadTest {
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
        final Queue<Integer> queue = new ConcurrentLinkedQueue<Integer>();

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

        DurationMeasurement durationMeasurement = DurationMeasurement.startMeasurementNow();
        readThread.start();
        writeThread.start();
        readThread.join();
        writeThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return durationMeasurement.durationUntilNow();
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

        DurationMeasurement durationMeasurement = DurationMeasurement.startMeasurementNow();
        readThread.start();
        writeThread.start();
        readThread.join();
        writeThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return durationMeasurement.durationUntilNow();
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

        DurationMeasurement durationMeasurement = DurationMeasurement.startMeasurementNow();
        readThread.start();
        writeThread.start();
        readThread.join();
        writeThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return durationMeasurement.durationUntilNow();
    }

    @Test
    public void completionTimeServicesShouldBehaveDeterministically() throws InterruptedException, ExecutionException, WorkloadException, CompletionTimeException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        String otherPeerId = "somePeer";
        List<String> peerIds = Lists.newArrayList(otherPeerId);
        int testRepetitions = 10;
        long syncDuration;
        long threadedDuration;

        for (int workerThreads = 1; workerThreads < 65; workerThreads = workerThreads * 2) {
            syncDuration = 0;
            for (int i = 0; i < testRepetitions; i++) {
                CompletionTimeService completionTimeService = new NaiveSynchronizedCompletionTimeService(peerIds);
                syncDuration += coordinationCompletionTimeServiceTest(completionTimeService, otherPeerId, errorReporter, workerThreads).asMilli();
                completionTimeService.shutdown();
            }
            System.out.printf("Threads:%-2s\t%s=%s",
                    workerThreads,
                    NaiveSynchronizedCompletionTimeService.class.getSimpleName(),
                    Duration.fromMilli(syncDuration / testRepetitions).toString());
            threadedDuration = 0;
            for (int i = 0; i < testRepetitions; i++) {
                CompletionTimeService completionTimeService = new ThreadedQueuedCompletionTimeService(peerIds, errorReporter);
                threadedDuration += coordinationCompletionTimeServiceTest(completionTimeService, otherPeerId, errorReporter, workerThreads).asMilli();
                completionTimeService.shutdown();
            }
            System.out.printf("\t%s=%s\n",
                    ThreadedQueuedCompletionTimeService.class.getSimpleName(),
                    Duration.fromMilli(threadedDuration / testRepetitions).toString());
        }
    }

    public Duration coordinationCompletionTimeServiceTest(CompletionTimeService completionTimeService,
                                                          String otherPeerId,
                                                          ConcurrentErrorReporter errorReporter,
                                                          int workerThreadCount)
            throws WorkloadException, InterruptedException, ExecutionException, CompletionTimeException {
        // initialize executor
        int threadCount = workerThreadCount;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount, threadFactory);
        CompletionService<Integer> executorCompletionService = new ExecutorCompletionService<Integer>(executorService);

        // initialize workload
        long operationCount = 100000;
        int operationCountCheckPoint1 = 100;
        int operationCountCheckPoint2 = 900;
        assertThat(operationCount > operationCountCheckPoint1 + operationCountCheckPoint2, is(true));
        WorkloadParams params =
                new WorkloadParams(null, null, null, operationCount, threadCount, false, null, null);
        Workload workload = new SimpleWorkload();
        workload.init(params);
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.getOperations(generators);

        // run
        DurationMeasurement duration = DurationMeasurement.startMeasurementNow();

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
            executorCompletionService.submit(new GctAccessingCallable(operation, completionTimeService, errorReporter));
        }

        int completedTasks = 1;
        while (completedTasks < operationCountCheckPoint1) {
            executorCompletionService.take();
            completedTasks++;
        }

        Future<Time> gctFuture1 = completionTimeService.globalCompletionTimeFuture();
        assertThat(gctFuture1.get(), equalTo(gctCheckpointOperation1.scheduledStartTime()));

        Operation<?> gctCheckpointOperation2 = operations.next();
        completionTimeService.submitExternalCompletionTime(otherPeerId, gctCheckpointOperation2.scheduledStartTime());
        completionTimeService.submitInitiatedTime(gctCheckpointOperation2.scheduledStartTime());
        executorCompletionService.submit(new GctAccessingCallable(gctCheckpointOperation2, completionTimeService, errorReporter));

        for (int i = operationCountCheckPoint1 + 1; i < operationCountCheckPoint2; i++) {
            Operation<?> operation = operations.next();
            assertThat(operation.scheduledStartTime().asMilli() >= lastScheduledStartTime.asMilli(), is(true));
            lastScheduledStartTime = operation.scheduledStartTime();
            completionTimeService.submitInitiatedTime(operation.scheduledStartTime());
            executorCompletionService.submit(new GctAccessingCallable(operation, completionTimeService, errorReporter));
        }

        // Wait for tasks to finish submitting CompletedTimes
        completedTasks = 0;
        while (completedTasks < operationCountCheckPoint2 - operationCountCheckPoint1) {
            executorCompletionService.take();
            completedTasks++;
        }

        Future<Time> gctFuture2 = completionTimeService.globalCompletionTimeFuture();
        assertThat(gctFuture2.get(), equalTo(gctCheckpointOperation2.scheduledStartTime()));

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            // Just to make sure that time stamps are in fact increasing
            assertThat(operation.scheduledStartTime().greatThan(lastScheduledStartTime), is(true));
            lastScheduledStartTime = operation.scheduledStartTime();
            // TODO document
            // IMPORTANT: queue initiated event BEFORE submitting task
            // This ensures that the event is initiated before later events initiate AND complete
            // Otherwise race conditions can cause GCT to proceed before an earlier initiated time is logged
            completionTimeService.submitInitiatedTime(operation.scheduledStartTime());
            executorCompletionService.submit(new GctAccessingCallable(operation, completionTimeService, errorReporter));
        }

        // Wait for tasks to finish submitting CompletedTimes
        completedTasks = 0;
        while (completedTasks < operationCount - operationCountCheckPoint2) {
            executorCompletionService.take();
            completedTasks++;
        }

        completionTimeService.submitExternalCompletionTime(otherPeerId, lastScheduledStartTime);

        Future<Time> gctFuture3 = completionTimeService.globalCompletionTimeFuture();
        assertThat(gctFuture3.get(), equalTo(lastScheduledStartTime));

        Duration testDuration = duration.durationUntilNow();

        executorService.shutdown();
        boolean allTasksCompletedInTime = executorService.awaitTermination(1, TimeUnit.SECONDS);
        assertThat(allTasksCompletedInTime, is(true));
        assertThat(errorReporter.errorEncountered(), is(false));
        return testDuration;
    }

    class GctAccessingCallable implements Callable<Integer> {
        private final Operation<?> operation;
        private final CompletionTimeService completionTimeService;
        private final ConcurrentErrorReporter errorReporter;

        public GctAccessingCallable(Operation<?> operation,
                                    CompletionTimeService completionTimeService,
                                    ConcurrentErrorReporter errorReporter) {
            this.operation = operation;
            this.completionTimeService = completionTimeService;
            this.errorReporter = errorReporter;
        }

        public Integer call() throws Exception {
            try {
                Time gctTime = completionTimeService.globalCompletionTime();
                assertThat(gctTime, notNullValue());
                // TODO in real application this should be done AFTER completion of operation
                // TODO write lessons learnt into Confluence specification document
                completionTimeService.submitCompletedTime(operation.scheduledStartTime());
                return 1;
            } catch (Exception e) {
                errorReporter.reportError(this, "Error in call()");
                return -1;
            }
        }
    }

}
