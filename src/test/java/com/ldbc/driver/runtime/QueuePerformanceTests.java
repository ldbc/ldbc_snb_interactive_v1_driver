package com.ldbc.driver.runtime;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
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
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

@Ignore
public class QueuePerformanceTests {
    static Map<String, String> defaultSnbParamsMapWithParametersDir() {
        Map<String, String> additionalParams = new HashMap<>();
        additionalParams.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        return MapUtils.mergeMaps(
                LdbcSnbInteractiveWorkload.defaultReadOnlyConfig(),
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
    public void x() {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        List<Operation<?>> stream0 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(0), Time.fromMilli(0), "0-1"),
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "0-2"),
                new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "0-3"),
                new TimedNamedOperation1(Time.fromMilli(6), Time.fromMilli(0), "0-4"),
                new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(0), "0-5")
        );

        List<Operation<?>> stream1 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(0), Time.fromMilli(0), "1-1"),
                new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "1-2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "1-3"),
                new TimedNamedOperation1(Time.fromMilli(9), Time.fromMilli(0), "1-4")
        );

        List<Operation<?>> stream2 = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "2-1"),
                new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "2-2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "2-3"),
                new TimedNamedOperation1(Time.fromMilli(8), Time.fromMilli(0), "2-4"),
                new TimedNamedOperation1(Time.fromMilli(8), Time.fromMilli(0), "2-5"),
                new TimedNamedOperation1(Time.fromMilli(9), Time.fromMilli(0), "2-6")
        );

        List<Operation<?>> stream3 = Lists.newArrayList(
        );

        List<Operation<?>> stream4 = Lists.newArrayList(gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(Time.fromMilli(10), Duration.fromMilli(1)),
                                gf.constant(Time.fromMilli(0)),
                                gf.constant("4-x")
                        ),
                        1000000
                )
        );

        List<Iterator<Operation<?>>> streams = Lists.newArrayList(
                stream0.iterator(),
                stream1.iterator(),
                stream2.iterator(),
                stream3.iterator(),
                stream4.iterator()
        );

        long k = 10;
        long[] kForIterator = fromAmongAllRetrieveTopK(streams, k);

        List<Operation<?>> topK = Lists.newArrayList(
                gf.mergeSortOperationsByStartTime(
                        gf.limit(
                                stream0.iterator(),
                                kForIterator[0]
                        ),
                        gf.limit(
                                stream1.iterator(),
                                kForIterator[1]
                        ),
                        gf.limit(
                                stream2.iterator(),
                                kForIterator[2]
                        ),
                        gf.limit(
                                stream3.iterator(),
                                kForIterator[3]
                        ),
                        gf.limit(
                                stream4.iterator(),
                                kForIterator[4]
                        )
                )
        );

        for (Operation<?> operation : topK) {
            System.out.println(((TimedNamedOperation1) operation).name() + " " + operation.scheduledStartTime().asMilli());
        }

        assertThat((long) topK.size(), is(k));
        assertThat(((TimedNamedOperation1) topK.get(0)).name(), anyOf(equalTo("0-1"), equalTo("1-1")));
        assertThat(((TimedNamedOperation1) topK.get(1)).name(), anyOf(equalTo("0-1"), equalTo("1-1")));
        assertThat(((TimedNamedOperation1) topK.get(0)).name(), not(equalTo(((TimedNamedOperation1) topK.get(1)).name())));
        assertThat(((TimedNamedOperation1) topK.get(2)).name(), anyOf(equalTo("0-2"), equalTo("2-1")));
        assertThat(((TimedNamedOperation1) topK.get(3)).name(), anyOf(equalTo("0-2"), equalTo("2-1")));
        assertThat(((TimedNamedOperation1) topK.get(2)).name(), not(equalTo(((TimedNamedOperation1) topK.get(3)).name())));
        assertThat(((TimedNamedOperation1) topK.get(4)).name(), anyOf(equalTo("0-3")));
        assertThat(((TimedNamedOperation1) topK.get(5)).name(), anyOf(equalTo("1-2"), equalTo("2-2")));
        assertThat(((TimedNamedOperation1) topK.get(6)).name(), anyOf(equalTo("1-2"), equalTo("2-2")));
        assertThat(((TimedNamedOperation1) topK.get(5)).name(), not(equalTo(((TimedNamedOperation1) topK.get(6)).name())));
        assertThat(((TimedNamedOperation1) topK.get(7)).name(), anyOf(equalTo("1-3"), equalTo("2-3")));
        assertThat(((TimedNamedOperation1) topK.get(8)).name(), anyOf(equalTo("1-3"), equalTo("2-3")));
        assertThat(((TimedNamedOperation1) topK.get(7)).name(), not(equalTo(((TimedNamedOperation1) topK.get(8)).name())));
        assertThat(((TimedNamedOperation1) topK.get(9)).name(), anyOf(equalTo("0-4")));
    }

    private long[] fromAmongAllRetrieveTopK(List<Iterator<Operation<?>>> iterators, long k) {
        long kSoFar = 0;
        long[] kForIterator = new long[iterators.size()];
        for (int i = 0; i < iterators.size(); i++) {
            kForIterator[i] = 0;
        }
        Operation[] iteratorHeads = new Operation[iterators.size()];
        for (int i = 0; i < iterators.size(); i++) {
            iteratorHeads[i] = null;
        }
        while (kSoFar < k) {
            long minNano = Long.MAX_VALUE;
            int indexOfMin = -1;
            for (int i = 0; i < iterators.size(); i++) {
                if (null == iteratorHeads[i] && iterators.get(i).hasNext()) {
                    iteratorHeads[i] = iterators.get(i).next();
                }
                if (null != iteratorHeads[i] && iteratorHeads[i].scheduledStartTime().asNano() < minNano) {
                    minNano = iteratorHeads[i].scheduledStartTime().asNano();
                    indexOfMin = i;
                }
            }
            kForIterator[indexOfMin] = kForIterator[indexOfMin] + 1;
            iteratorHeads[indexOfMin] = null;
            kSoFar = kSoFar + 1;
        }
        return kForIterator;
    }

    @Test
    public void operationQueuePerformanceTest() throws WorkloadException, InterruptedException {
        // Given
        Map<String, String> paramsMap = defaultSnbParamsMapWithParametersDir();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
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

        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(config);

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.operations(gf, config.operationCount());

//        System.out.println("Materializing...");
//        List operationsList = ImmutableList.copyOf(operations);
//        operationsList.size();
//        operations = operationsList.iterator();
        System.out.println("Benchmarking...");

//        Duration duration = doOperationQueuePerformanceTest(operations, DefaultQueues.<Operation<?>>newBlockingBounded(1000));
        Duration duration = doOperationQueuePerformanceTest(operations, DefaultQueues.<Operation<?>>newBlockingBounded(10000));
        long opsPerSecond = Math.round(((double) config.operationCount() / duration.asNano()) * 1000000000);
        System.out.println(String.format("%s operations in %s: %s op/sec", config.operationCount(), duration, opsPerSecond));
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
