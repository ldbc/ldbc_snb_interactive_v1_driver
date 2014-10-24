package com.ldbc.driver;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.validation.ClassNameWorkloadFactory;
import com.ldbc.driver.validation.WorkloadFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class WorkloadStreams {
    private WorkloadStreamDefinition asynchronousStream = null;
    private List<WorkloadStreamDefinition> blockingStreams = new ArrayList<>();

    public static WorkloadStreams timeOffsetAndCompressWorkloadStreams(WorkloadStreams originalWorkloadStreams,
                                                                       long newStartTimeAsMilli,
                                                                       double compressionRatio,
                                                                       GeneratorFactory gf) throws WorkloadException {
        TemporalUtil temporalUtil = new TemporalUtil();
        long minScheduledStartTimeAsMilli = Long.MAX_VALUE;

        /*
         * Find earliest scheduled start time from across all streams
         */

        PeekingIterator<Operation<?>> peekingAsyncDependencyOperationStream = Iterators.peekingIterator(originalWorkloadStreams.asynchronousStream().dependencyOperations());
        try {
            long firstAsMilli = peekingAsyncDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            if (firstAsMilli < minScheduledStartTimeAsMilli) minScheduledStartTimeAsMilli = firstAsMilli;
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        PeekingIterator<Operation<?>> peekingAsyncNonDependencyOperationStream = Iterators.peekingIterator(originalWorkloadStreams.asynchronousStream().nonDependencyOperations());
        try {
            long firstAsMilli = peekingAsyncNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            if (firstAsMilli < minScheduledStartTimeAsMilli) minScheduledStartTimeAsMilli = firstAsMilli;
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        List<Long> peekingBlockingDependencyOperationStreamsAheadOfMinByMillis = new ArrayList<>();
        List<PeekingIterator<Operation<?>>> peekingBlockingDependencyOperationStreams = new ArrayList<>();
        List<Long> peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis = new ArrayList<>();
        List<PeekingIterator<Operation<?>>> peekingBlockingNonDependencyOperationStreams = new ArrayList<>();
        List<WorkloadStreamDefinition> blockingStreams = originalWorkloadStreams.blockingStreamDefinitions();
        for (int i = 0; i < blockingStreams.size(); i++) {
            PeekingIterator<Operation<?>> peekingBlockingDependencyOperationStream = Iterators.peekingIterator(blockingStreams.get(i).dependencyOperations());
            try {
                long firstAsMilli = peekingBlockingDependencyOperationStream.peek().scheduledStartTimeAsMilli();
                if (firstAsMilli < minScheduledStartTimeAsMilli) minScheduledStartTimeAsMilli = firstAsMilli;
            } catch (NoSuchElementException e) {
                // do nothing, just means stream was empty
            }
            peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.add(0l);
            peekingBlockingDependencyOperationStreams.add(peekingBlockingDependencyOperationStream);

            PeekingIterator<Operation<?>> peekingBlockingNonDependencyOperationStream = Iterators.peekingIterator(blockingStreams.get(i).nonDependencyOperations());
            try {
                long firstAsMilli = peekingBlockingNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
                if (firstAsMilli < minScheduledStartTimeAsMilli) minScheduledStartTimeAsMilli = firstAsMilli;
            } catch (NoSuchElementException e) {
                // do nothing, just means stream was empty
            }
            peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis.add(0l);
            peekingBlockingNonDependencyOperationStreams.add(peekingBlockingNonDependencyOperationStream);
        }

        if (Long.MAX_VALUE == minScheduledStartTimeAsMilli) minScheduledStartTimeAsMilli = newStartTimeAsMilli;

        /*
         * Find how far ahead of earliest scheduled start time each stream is when it starts
         */

        long peekingAsyncDependencyOperationStreamAheadOfMinByAsMilli = 0;
        try {
            long firstAsMilli = peekingAsyncDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            peekingAsyncDependencyOperationStreamAheadOfMinByAsMilli = temporalUtil.convert(
                    Math.round(temporalUtil.convert(firstAsMilli - minScheduledStartTimeAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) * compressionRatio),
                    TimeUnit.NANOSECONDS,
                    TimeUnit.MILLISECONDS);
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        long peekingAsyncNonDependencyOperationStreamAheadOfMinByAsMilli = 0l;
        try {
            long firstAsMilli = peekingAsyncNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            peekingAsyncNonDependencyOperationStreamAheadOfMinByAsMilli = temporalUtil.convert(
                    Math.round(temporalUtil.convert(firstAsMilli - minScheduledStartTimeAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) * compressionRatio),
                    TimeUnit.NANOSECONDS,
                    TimeUnit.MILLISECONDS);
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        for (int i = 0; i < peekingBlockingDependencyOperationStreams.size(); i++) {
            try {
                long firstAsMilli = peekingBlockingDependencyOperationStreams.get(i).peek().scheduledStartTimeAsMilli();
                peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.set(
                        i,
                        temporalUtil.convert(
                                Math.round(temporalUtil.convert(firstAsMilli - minScheduledStartTimeAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) * compressionRatio),
                                TimeUnit.NANOSECONDS,
                                TimeUnit.MILLISECONDS)
                );
            } catch (NoSuchElementException e) {
                // do nothing, just means stream was empty
            }
        }

        for (int i = 0; i < peekingBlockingNonDependencyOperationStreams.size(); i++) {
            try {
                long firstAsMilli = peekingBlockingNonDependencyOperationStreams.get(i).peek().scheduledStartTimeAsMilli();
                peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis.set(
                        i,
                        temporalUtil.convert(
                                Math.round(temporalUtil.convert(firstAsMilli - minScheduledStartTimeAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS) * compressionRatio),
                                TimeUnit.NANOSECONDS,
                                TimeUnit.MILLISECONDS)
                );
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
                        newStartTimeAsMilli + peekingAsyncDependencyOperationStreamAheadOfMinByAsMilli,
                        compressionRatio
                ),
                gf.timeOffsetAndCompress(
                        peekingAsyncNonDependencyOperationStream,
                        newStartTimeAsMilli + peekingAsyncNonDependencyOperationStreamAheadOfMinByAsMilli,
                        compressionRatio
                )
        );

        for (int i = 0; i < blockingStreams.size(); i++) {
            timeOffsetAndCompressedWorkloadStreams.addBlockingStream(
                    blockingStreams.get(i).dependentOperationTypes(),
                    gf.timeOffsetAndCompress(
                            peekingBlockingDependencyOperationStreams.get(i),
                            newStartTimeAsMilli + peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.get(i),
                            compressionRatio
                    ),
                    gf.timeOffsetAndCompress(
                            peekingBlockingNonDependencyOperationStreams.get(i),
                            newStartTimeAsMilli + peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis.get(i),
                            compressionRatio
                    )
            );
        }

        return timeOffsetAndCompressedWorkloadStreams;
    }

    public static Tuple.Tuple2<WorkloadStreams, Workload> createNewWorkloadWithLimitedWorkloadStreams(DriverConfiguration configuration, GeneratorFactory gf) throws WorkloadException, IOException {
        ClassNameWorkloadFactory workloadFactory = new ClassNameWorkloadFactory(configuration.workloadClassName());
        return createNewWorkloadWithLimitedWorkloadStreams(workloadFactory, configuration, gf);
    }

    public static Tuple.Tuple2<WorkloadStreams, Workload> createNewWorkloadWithLimitedWorkloadStreams(WorkloadFactory workloadFactory, DriverConfiguration configuration, GeneratorFactory gf) throws WorkloadException, IOException {
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
        workload.close();
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
        TemporalUtil temporalUtil = new TemporalUtil();
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
                    long streamHeadTimeAsMilli = streamHeads[i].scheduledStartTimeAsMilli();
                    if (-1 == streamHeadTimeAsMilli)
                        throw new WorkloadException(String.format("Operation must have start time\n%s", streamHeads[i]));
                    long streamHeadTimeAsNano = temporalUtil.convert(streamHeadTimeAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS);
                    if (null != streamHeads[i] && streamHeadTimeAsNano < minNano) {
                        minNano = streamHeadTimeAsNano;
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
