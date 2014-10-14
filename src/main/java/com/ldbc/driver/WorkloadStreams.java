package com.ldbc.driver;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.validation.ClassNameWorkloadFactory;
import com.ldbc.driver.validation.WorkloadFactory;

import java.util.*;

public class WorkloadStreams {
    private WorkloadStreamDefinition asynchronousStream = null;
    private List<WorkloadStreamDefinition> blockingStreams = new ArrayList<>();

    public static WorkloadStreams timeOffsetAndCompressWorkloadStreams(WorkloadStreams originalWorkloadStreams,
                                                                       Time newStartTime,
                                                                       double compressionRatio,
                                                                       GeneratorFactory gf) throws WorkloadException {
        Time minScheduledStartTime = null;

        /*
         * Find earliest scheduled start time from across all streams
         */

        Duration peekingAsyncDependencyOperationStreamAheadOfMinBy = Duration.fromNano(0);
        PeekingIterator<Operation<?>> peekingAsyncDependencyOperationStream = Iterators.peekingIterator(originalWorkloadStreams.asynchronousStream().dependencyOperations());
        try {
            Time first = peekingAsyncDependencyOperationStream.peek().scheduledStartTime();
            if (null == minScheduledStartTime || first.lt(minScheduledStartTime)) minScheduledStartTime = first;
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        Duration peekingAsyncNonDependencyOperationStreamAheadOfMinBy = Duration.fromNano(0);
        PeekingIterator<Operation<?>> peekingAsyncNonDependencyOperationStream = Iterators.peekingIterator(originalWorkloadStreams.asynchronousStream().nonDependencyOperations());
        try {
            Time first = peekingAsyncNonDependencyOperationStream.peek().scheduledStartTime();
            if (null == minScheduledStartTime || first.lt(minScheduledStartTime)) minScheduledStartTime = first;
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        List<Duration> peekingBlockingDependencyOperationStreamsAheadOfMinBy = new ArrayList<>();
        List<PeekingIterator<Operation<?>>> peekingBlockingDependencyOperationStreams = new ArrayList<>();
        List<Duration> peekingBlockingNonDependencyOperationStreamsAheadOfMinBy = new ArrayList<>();
        List<PeekingIterator<Operation<?>>> peekingBlockingNonDependencyOperationStreams = new ArrayList<>();
        List<WorkloadStreamDefinition> blockingStreams = originalWorkloadStreams.blockingStreamDefinitions();
        for (int i = 0; i < blockingStreams.size(); i++) {
            PeekingIterator<Operation<?>> peekingBlockingDependencyOperationStream = Iterators.peekingIterator(blockingStreams.get(i).dependencyOperations());
            try {
                Time first = peekingBlockingDependencyOperationStream.peek().scheduledStartTime();
                if (null == minScheduledStartTime || first.lt(minScheduledStartTime)) minScheduledStartTime = first;
            } catch (NoSuchElementException e) {
                // do nothing, just means stream was empty
            }
            peekingBlockingDependencyOperationStreamsAheadOfMinBy.add(Duration.fromNano(0));
            peekingBlockingDependencyOperationStreams.add(peekingBlockingDependencyOperationStream);

            PeekingIterator<Operation<?>> peekingBlockingNonDependencyOperationStream = Iterators.peekingIterator(blockingStreams.get(i).nonDependencyOperations());
            try {
                Time first = peekingBlockingNonDependencyOperationStream.peek().scheduledStartTime();
                if (null == minScheduledStartTime || first.lt(minScheduledStartTime)) minScheduledStartTime = first;
            } catch (NoSuchElementException e) {
                // do nothing, just means stream was empty
            }
            peekingBlockingNonDependencyOperationStreamsAheadOfMinBy.add(Duration.fromNano(0));
            peekingBlockingNonDependencyOperationStreams.add(peekingBlockingNonDependencyOperationStream);
        }

        if (null == minScheduledStartTime) minScheduledStartTime = newStartTime;

        /*
         * Find how far ahead of earliest scheduled start time each stream is when it starts
         */

        try {
            Time first = peekingAsyncDependencyOperationStream.peek().scheduledStartTime();
            peekingAsyncDependencyOperationStreamAheadOfMinBy = Duration.fromNano(Math.round(first.durationGreaterThan(minScheduledStartTime).asNano() * compressionRatio));
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        try {
            Time first = peekingAsyncNonDependencyOperationStream.peek().scheduledStartTime();
            peekingAsyncNonDependencyOperationStreamAheadOfMinBy = Duration.fromNano(Math.round(first.durationGreaterThan(minScheduledStartTime).asNano() * compressionRatio));
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        for (int i = 0; i < peekingBlockingDependencyOperationStreams.size(); i++) {
            try {
                Time first = peekingBlockingDependencyOperationStreams.get(i).peek().scheduledStartTime();
                peekingBlockingDependencyOperationStreamsAheadOfMinBy.set(i, Duration.fromNano(Math.round(first.durationGreaterThan(minScheduledStartTime).asNano() * compressionRatio)));
            } catch (NoSuchElementException e) {
                // do nothing, just means stream was empty
            }
        }

        for (int i = 0; i < peekingBlockingNonDependencyOperationStreams.size(); i++) {
            try {
                Time first = peekingBlockingNonDependencyOperationStreams.get(i).peek().scheduledStartTime();
                peekingBlockingNonDependencyOperationStreamsAheadOfMinBy.set(i, Duration.fromNano(Math.round(first.durationGreaterThan(minScheduledStartTime).asNano() * compressionRatio)));
            } catch (NoSuchElementException e) {
                // do nothing, just means stream was empty
            }
        }

        /*
         * copy unbounded streams to new workload streams instance, applying offset and time compression
         */

        WorkloadStreams timeOffsetAndCompressedWorkloadStreams = new WorkloadStreams();

        timeOffsetAndCompressedWorkloadStreams.setAsynchronousStream(
                originalWorkloadStreams.asynchronousStream().dependentOperationTypes(),
                gf.timeOffsetAndCompress(
                        peekingAsyncDependencyOperationStream,
                        newStartTime.plus(peekingAsyncDependencyOperationStreamAheadOfMinBy),
                        compressionRatio
                ),
                gf.timeOffsetAndCompress(
                        peekingAsyncNonDependencyOperationStream,
                        newStartTime.plus(peekingAsyncNonDependencyOperationStreamAheadOfMinBy),
                        compressionRatio
                )
        );

        for (int i = 0; i < blockingStreams.size(); i++) {
            timeOffsetAndCompressedWorkloadStreams.addBlockingStream(
                    blockingStreams.get(i).dependentOperationTypes(),
                    gf.timeOffsetAndCompress(
                            peekingBlockingDependencyOperationStreams.get(i),
                            newStartTime.plus(peekingBlockingDependencyOperationStreamsAheadOfMinBy.get(i)),
                            compressionRatio
                    ),
                    gf.timeOffsetAndCompress(
                            peekingBlockingNonDependencyOperationStreams.get(i),
                            newStartTime.plus(peekingBlockingNonDependencyOperationStreamsAheadOfMinBy.get(i)),
                            compressionRatio
                    )
            );
        }

        return timeOffsetAndCompressedWorkloadStreams;
    }

    public static Tuple.Tuple2<WorkloadStreams, Workload> createNewWorkloadWithLimitedWorkloadStreams(DriverConfiguration configuration, GeneratorFactory gf) throws WorkloadException {
        ClassNameWorkloadFactory workloadFactory = new ClassNameWorkloadFactory(configuration.workloadClassName());
        return createNewWorkloadWithLimitedWorkloadStreams(workloadFactory, configuration, gf);
    }

    public static Tuple.Tuple2<WorkloadStreams, Workload> createNewWorkloadWithLimitedWorkloadStreams(WorkloadFactory workloadFactory, DriverConfiguration configuration, GeneratorFactory gf) throws WorkloadException {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        // get workload
        Workload workload = workloadFactory.createWorkload();
        workload.init(configuration);
        // retrieve unbounded streams
        WorkloadStreams unlimitedWorkloadStreams = workload.streams(gf);
        List<Iterator<Operation<?>>> streams = new ArrayList<>();
        streams.add(unlimitedWorkloadStreams.asynchronousStream().dependencyOperations());
        streams.add(unlimitedWorkloadStreams.asynchronousStream().nonDependencyOperations());
        for (WorkloadStreamDefinition stream : unlimitedWorkloadStreams.blockingStreamDefinitions()) {
            streams.add(stream.dependencyOperations());
            streams.add(stream.nonDependencyOperations());
        }
        // stream through streams once, to calculate how many operations are needed from each, to get operation_count in total
        long[] limitForStream = WorkloadStreams.fromAmongAllRetrieveTopK(streams, configuration.operationCount());
        workload.cleanup();
        // reinitialize workload, so it can be streamed through from the beginning
        workload = workloadFactory.createWorkload();
        workload.init(configuration);
        // retrieve unbounded streams
        unlimitedWorkloadStreams = workload.streams(gf);
        // copy unbounded streams to new workload streams instance, applying limits we just computed
        workloadStreams.setAsynchronousStream(
                unlimitedWorkloadStreams.asynchronousStream().dependentOperationTypes(),
                gf.limit(unlimitedWorkloadStreams.asynchronousStream().dependencyOperations(), limitForStream[0]),
                gf.limit(unlimitedWorkloadStreams.asynchronousStream().nonDependencyOperations(), limitForStream[1])
        );
        List<WorkloadStreamDefinition> blockingStreams = unlimitedWorkloadStreams.blockingStreamDefinitions();
        for (int i = 0; i < blockingStreams.size(); i++) {
            workloadStreams.addBlockingStream(
                    blockingStreams.get(i).dependentOperationTypes(),
                    gf.limit(blockingStreams.get(i).dependencyOperations(), limitForStream[i * 2 + 2]),
                    gf.limit(blockingStreams.get(i).nonDependencyOperations(), limitForStream[i * 2 + 3])
            );
        }
        return Tuple.tuple2(workloadStreams, workload);
    }

    public static long[] fromAmongAllRetrieveTopK(List<Iterator<Operation<?>>> streams, long k) throws WorkloadException {
        long kSoFar = 0;
        long[] kForStream = new long[streams.size()];
        for (int i = 0; i < streams.size(); i++) {
            kForStream[i] = 0;
        }
        Operation[] streamHeads = new Operation[streams.size()];
        for (int i = 0; i < streams.size(); i++) {
            streamHeads[i] = null;
        }
        while (kSoFar < k) {
            long minNano = Long.MAX_VALUE;
            int indexOfMin = -1;
            for (int i = 0; i < streams.size(); i++) {
                if (null != streamHeads[i] || streams.get(i).hasNext()) {
                    if (null == streamHeads[i]) {
                        streamHeads[i] = streams.get(i).next();
                    }
                    Time streamHeadTime = streamHeads[i].scheduledStartTime();
                    if (null == streamHeadTime)
                        throw new WorkloadException(String.format("Operation must have start time\n%s", streamHeads[i]));
                    if (null != streamHeads[i] && streamHeadTime.asNano() < minNano) {
                        minNano = streamHeadTime.asNano();
                        indexOfMin = i;
                    }
                }
            }
            if (-1 == indexOfMin) {
                // iterators are empty, nothing left to retrieve
                break;
            }
            kForStream[indexOfMin] = kForStream[indexOfMin] + 1;
            streamHeads[indexOfMin] = null;
            kSoFar = kSoFar + 1;
        }
        return kForStream;
    }

    public WorkloadStreamDefinition asynchronousStream() {
        return (null != asynchronousStream)
                ?
                asynchronousStream
                :
                new WorkloadStreamDefinition(
                        new HashSet<Class<? extends Operation<?>>>(),
                        Collections.<Operation<?>>emptyIterator(),
                        Collections.<Operation<?>>emptyIterator()
                );
    }

    public void setAsynchronousStream(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                      Iterator<Operation<?>> dependencyOperations,
                                      Iterator<Operation<?>> nonDependencyOperations) {
        this.asynchronousStream = new WorkloadStreamDefinition(dependentOperationTypes, dependencyOperations, nonDependencyOperations);
    }

    public List<WorkloadStreamDefinition> blockingStreamDefinitions() {
        return blockingStreams;
    }

    public void addBlockingStream(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                  Iterator<Operation<?>> dependencyOperations,
                                  Iterator<Operation<?>> nonDependencyOperations) {
        WorkloadStreamDefinition blockingStream = new WorkloadStreamDefinition(dependentOperationTypes, dependencyOperations, nonDependencyOperations);
        this.blockingStreams.add(blockingStream);
    }

    public WorkloadStreams applyTimeOffsetAndCompressionRatio(GeneratorFactory gf, Time newStartTime, double timeCompressionRatio) {
        // TODO test
        // TODO actually apply time shift logic
        WorkloadStreams workloadStreams = new WorkloadStreams();
        for (WorkloadStreamDefinition streamDefinition : blockingStreamDefinitions()) {
            workloadStreams.addBlockingStream(
                    streamDefinition.dependentOperationTypes(),
                    streamDefinition.dependencyOperations(),
                    streamDefinition.nonDependencyOperations()
            );
        }
        workloadStreams.setAsynchronousStream(
                asynchronousStream().dependentOperationTypes(),
                asynchronousStream().dependencyOperations(),
                asynchronousStream().nonDependencyOperations()
        );
        return workloadStreams;
    }

    public Iterator<Operation<?>> mergeSortedByStartTime(GeneratorFactory gf) {
        // TODO test
        List<Iterator<Operation<?>>> allStreams = new ArrayList<>();
        for (WorkloadStreamDefinition streamDefinition : blockingStreamDefinitions()) {
            allStreams.add(streamDefinition.dependencyOperations());
            allStreams.add(streamDefinition.nonDependencyOperations());
        }
        allStreams.add(asynchronousStream().dependencyOperations());
        allStreams.add(asynchronousStream().nonDependencyOperations());
        return gf.mergeSortOperationsByStartTime(allStreams.toArray(new Iterator[allStreams.size()]));
    }

    public static class WorkloadStreamDefinition {
        private final Set<Class<? extends Operation<?>>> dependentOperationTypes;
        private final Iterator<Operation<?>> dependencyOperations;
        private final Iterator<Operation<?>> nonDependencyOperations;

        public WorkloadStreamDefinition(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                        Iterator<Operation<?>> dependencyOperations,
                                        Iterator<Operation<?>> nonDependencyOperations) {
            this.dependentOperationTypes = dependentOperationTypes;
            this.dependencyOperations = dependencyOperations;
            this.nonDependencyOperations = nonDependencyOperations;
        }

        public Iterator<Operation<?>> dependencyOperations() {
            return (null != dependencyOperations) ? dependencyOperations : Collections.<Operation<?>>emptyIterator();
        }

        public Iterator<Operation<?>> nonDependencyOperations() {
            return (null != nonDependencyOperations) ? nonDependencyOperations : Collections.<Operation<?>>emptyIterator();
        }

        public Set<Class<? extends Operation<?>>> dependentOperationTypes() {
            return (null != dependentOperationTypes) ? dependentOperationTypes : new HashSet<Class<? extends Operation<?>>>();
        }
    }
}
