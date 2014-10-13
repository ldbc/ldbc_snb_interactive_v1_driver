package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@Ignore
public class QueuePerformanceTests {
    static Map<String, String> defaultSnbParamsMapWithParametersDir() {
        Map<String, String> additionalParams = new HashMap<>();
        additionalParams.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        return MapUtils.mergeMaps(
                LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig(),
                ConsoleAndFileDriverConfiguration.convertLongKeysToShortKeys(additionalParams),
                true);
    }

    final Operation<?> TERMINATE_OPERATION = new Operation<Object>() {
        @Override
        public Object marshalResult(String serializedOperationResult) throws SerializingMarshallingException {
            return null;
        }

        @Override
        public String serializeResult(Object operationResultInstance) throws SerializingMarshallingException {
            return null;
        }
    };

    final Integer TERMINATE = -1;
    final TimeSource timeSource = new SystemTimeSource();

    @Test
    public void operationQueuePerformanceTest() throws WorkloadException, InterruptedException {
        // Given
        Map<String, String> paramsMap = defaultSnbParamsMapWithParametersDir();
        // LDBC Interactive Workload-specific parameters
        // Driver-specific parameters
        String name = null;
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 10000000;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(0);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = null;
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromMinutes(60);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        boolean shouldCreateResultsLog = false;

        DriverConfiguration config = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                name,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                windowedExecutionWindowDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                shouldCreateResultsLog
        );

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Tuple.Tuple2<WorkloadStreams, Workload> workloadStreamsAndWorkload = WorkloadStreams.createNewWorkloadWithLimitedWorkloadStreams(config, gf);
        WorkloadStreams workloadStreams = workloadStreamsAndWorkload._1();
        Workload workload = workloadStreamsAndWorkload._2();

        Iterator<Operation<?>> operations = workloadStreams.mergeSortedByStartTime(gf);

        System.out.println("Benchmarking...");

//        Duration duration = doOperationQueuePerformanceTest(operations, DefaultQueues.<Operation<?>>newBlockingBounded(1000));
        Duration duration = doOperationQueuePerformanceTest(operations, DefaultQueues.<Operation<?>>newBlockingBounded(10000));
        long opsPerSecond = Math.round(((double) config.operationCount() / duration.asNano()) * 1000000000);
        System.out.println(String.format("%s operations in %s: %s op/sec", config.operationCount(), duration, opsPerSecond));
        workload.cleanup();
    }

    private Duration doOperationQueuePerformanceTest(final Iterator<Operation<?>> operations, final Queue<Operation<?>> queue) throws InterruptedException {
        final QueueEventSubmitter<Operation<?>> queueEventSubmitter = QueueEventSubmitter.queueEventSubmitterFor(queue);
        final QueueEventFetcher<Operation<?>> queueEventFetcher = QueueEventFetcher.queueEventFetcherFor(queue);
        Thread writeThread = new Thread() {
            @Override
            public void run() {
                while (operations.hasNext()) {
                    try {
                        queueEventSubmitter.submitEventToQueue(operations.next());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    queueEventSubmitter.submitEventToQueue(TERMINATE_OPERATION);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Producer finished");
            }
        };

        Thread readThread = new Thread() {
            @Override
            public void run() {
                Operation<?> operation = null;
                try {
                    operation = queueEventFetcher.fetchNextEvent();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                while (TERMINATE_OPERATION != operation) {
                    try {
                        operation = queueEventFetcher.fetchNextEvent();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("Consumer finished");
            }
        };

        Time startTime = timeSource.now();
        readThread.start();
        writeThread.start();
        writeThread.join();
        readThread.join();

        return timeSource.now().durationGreaterThan(startTime);
    }


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

        long startTimeAsMilli = timeSource.nowAsMilli();
        readThread.start();
        writeThread.start();
        writeThread.join();
        readThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return Duration.fromMilli(timeSource.nowAsMilli() - startTimeAsMilli);
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

        long startTimeAsMilli = timeSource.nowAsMilli();
        readThread.start();
        writeThread.start();
        writeThread.join();
        readThread.join();
        assertThat(queue.poll(), is(nullValue()));
        assertThat(queue.size(), is(0));
        return Duration.fromMilli(timeSource.nowAsMilli() - startTimeAsMilli);
    }
}
