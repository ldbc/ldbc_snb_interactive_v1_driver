package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import uk.co.real_logic.sbe.codec.java.DirectBuffer;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.LockSupport;

import static com.ldbc.driver.runtime.metrics.DisruptorMetricsEvent_NEW.*;

public class DisruptorConcurrentMetricsService_NEW implements ConcurrentMetricsService {
    private static final long SHUTDOWN_WAIT_TIMEOUT_AS_MILLI = TimeUnit.SECONDS.toMillis(30);

    public static final String RESULTS_LOG_FILENAME_SUFFIX = "-results_log.csv";
    public static final String RESULTS_METRICS_FILENAME_SUFFIX = "-results.json";
    public static final String RESULTS_CONFIGURATION_FILENAME_SUFFIX = "-configuration.properties";

    // TODO this could come from config, if we had a max_runtime parameter. for now, it can default to something
    public static final long DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO = TimeUnit.MINUTES.toNanos(90);

    private final AtomicLong initiatedEvents = new AtomicLong(0);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final TimeSource timeSource;
    private final RingBuffer<DirectBuffer> ringBuffer;
    private final Disruptor<DirectBuffer> disruptor;
    private final DisruptorMetricsEventHandler_NEW eventHandler;

    public DisruptorConcurrentMetricsService_NEW(TimeSource timeSource,
                                                 ConcurrentErrorReporter errorReporter,
                                                 TimeUnit timeUnit,
                                                 long maxRuntimeDurationAsNano,
                                                 SimpleCsvFileWriter csvResultsLogWriter,
                                                 Map<Integer, Class<? extends Operation<?>>> operationTypeToClassMapping) throws MetricsCollectionException {
        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = 1024;

        // Construct the Disruptor
        disruptor = new Disruptor(
                new MetricsCollectionEventFactory(),
                bufferSize,
                // Executor that will be used to construct new threads for consumers
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
//                new BlockingWaitStrategy()
                new LiteBlockingWaitStrategy()
//                new SleepingWaitStrategy()
//                new YieldingWaitStrategy()
//                new BusySpinWaitStrategy()
//                new TimeoutBlockingWaitStrategy()
//                new PhasedBackoffWaitStrategy()
        );

        // Connect the handler
        eventHandler = new DisruptorMetricsEventHandler_NEW(
                errorReporter,
                csvResultsLogWriter,
                timeUnit,
                timeSource,
                maxRuntimeDurationAsNano,
                operationTypeToClassMapping,
                DisruptorMetricsEvent_NEW.ACTING_BLOCK_LENGTH,
                DisruptorMetricsEvent_NEW.ACTING_VERSION,
                DisruptorMetricsEvent_NEW.MESSAGE_HEADER_SIZE
        );

        disruptor.handleEventsWith(eventHandler);
        DisruptorExceptionHandler_NEW exceptionHandler = new DisruptorExceptionHandler_NEW(errorReporter);
        disruptor.handleExceptionsFor(eventHandler).with(exceptionHandler);
        disruptor.handleExceptionsWith(exceptionHandler);

        // Start the Disruptor, starts all threads running  & get the ring buffer from the Disruptor to be used for publishing.
        ringBuffer = disruptor.start();

        this.timeSource = timeSource;
    }

    @Override
    public void submitOperationResult(
            int operationType,
            long scheduledStartTimeAsMilli,
            long actualStartTimeAsMilli,
            long runDurationAsNano,
            int resultCode
    ) throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Can not submit a result after calling shutdown");
        }
        initiatedEvents.incrementAndGet();
        ringBuffer.publishEvent(SET_AS_SUBMIT_OPERATION_RESULT, operationType, scheduledStartTimeAsMilli, actualStartTimeAsMilli, runDurationAsNano, resultCode);
    }

    @Override
    public WorkloadStatusSnapshot status() throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Can not read metrics status after calling shutdown");
        }
        AtomicStampedReference<WorkloadStatusSnapshot> statusSnapshotReference = eventHandler.statusSnapshot();
        int oldStamp = statusSnapshotReference.getStamp();
        ringBuffer.publishEvent(SET_AS_STATUS);
        while (statusSnapshotReference.getStamp() <= oldStamp) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
        return statusSnapshotReference.getReference();
    }

    @Override
    public WorkloadResultsSnapshot results() throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Can not retrieve results after calling shutdown");
        }
        AtomicStampedReference<WorkloadResultsSnapshot> resultsSnapshotReference = eventHandler.resultsSnapshot();
        int oldStamp = resultsSnapshotReference.getStamp();
        ringBuffer.publishEvent(SET_AS_REQUEST_WORKLOAD_RESULT);
        while (resultsSnapshotReference.getStamp() <= oldStamp) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
        return resultsSnapshotReference.getReference();
    }

    @Override
    synchronized public void shutdown() throws MetricsCollectionException {
        if (shutdown.get()) {
            throw new MetricsCollectionException("Metrics service has already been shutdown");
        }
        long startTimeMs = timeSource.nowAsMilli();
        boolean shutdownSuccessful = false;
        while (timeSource.nowAsMilli() - startTimeMs < SHUTDOWN_WAIT_TIMEOUT_AS_MILLI) {
            if (eventHandler.processedEventCount() >= initiatedEvents.get()) {
                shutdownSuccessful = true;
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
        }
        if (false == shutdownSuccessful) {
            String errMsg = String.format("%s timed out waiting for last operations to complete\n%s/%s operations completed",
                    getClass().getSimpleName(),
                    eventHandler.processedEventCount(),
                    initiatedEvents.get()
            );
            throw new MetricsCollectionException(errMsg);
        }
        try {
            disruptor.shutdown(SHUTDOWN_WAIT_TIMEOUT_AS_MILLI, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            String errMsg = String.format("%s timed out waiting for %s to shutdown",
                    getClass().getSimpleName(),
                    disruptor.getClass().getSimpleName()
            );
            throw new MetricsCollectionException(errMsg, e);
        }
        shutdown.set(true);
    }
}
