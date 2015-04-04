package com.ldbc.driver;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.validation.ClassNameWorkloadFactory;
import com.ldbc.driver.validation.WorkloadFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class WorkloadStreams {
    private WorkloadStreamDefinition asynchronousStream = null;
    private List<WorkloadStreamDefinition> blockingStreams = new ArrayList<>();

    public static WorkloadStreams timeOffsetAndCompressWorkloadStreams(WorkloadStreams originalWorkloadStreams,
                                                                       long newStartTimeAsMilli,
                                                                       double compressionRatio,
                                                                       GeneratorFactory gf) throws WorkloadException {
        long minScheduledStartTimeAsMilli = Long.MAX_VALUE;

        /*
         * Find earliest scheduled start time from across all streams
         */

        PeekingIterator<Operation<?>> peekingAsyncDependencyOperationStream = Iterators.peekingIterator(originalWorkloadStreams.asynchronousStream().dependencyOperations());
        try {
            long firstAsMilli = peekingAsyncDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            if (firstAsMilli < minScheduledStartTimeAsMilli)
                minScheduledStartTimeAsMilli = firstAsMilli;
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        PeekingIterator<Operation<?>> peekingAsyncNonDependencyOperationStream = Iterators.peekingIterator(originalWorkloadStreams.asynchronousStream().nonDependencyOperations());
        try {
            long firstAsMilli = peekingAsyncNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            if (firstAsMilli < minScheduledStartTimeAsMilli)
                minScheduledStartTimeAsMilli = firstAsMilli;
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
                if (firstAsMilli < minScheduledStartTimeAsMilli)
                    minScheduledStartTimeAsMilli = firstAsMilli;
            } catch (NoSuchElementException e) {
                // do nothing, just means stream was empty
            }
            peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.add(0l);
            peekingBlockingDependencyOperationStreams.add(peekingBlockingDependencyOperationStream);

            PeekingIterator<Operation<?>> peekingBlockingNonDependencyOperationStream = Iterators.peekingIterator(blockingStreams.get(i).nonDependencyOperations());
            try {
                long firstAsMilli = peekingBlockingNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
                if (firstAsMilli < minScheduledStartTimeAsMilli)
                    minScheduledStartTimeAsMilli = firstAsMilli;
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
            peekingAsyncDependencyOperationStreamAheadOfMinByAsMilli = Math.round((firstAsMilli - minScheduledStartTimeAsMilli) * compressionRatio);
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        long peekingAsyncNonDependencyOperationStreamAheadOfMinByAsMilli = 0l;
        try {
            long firstAsMilli = peekingAsyncNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            peekingAsyncNonDependencyOperationStreamAheadOfMinByAsMilli = Math.round((firstAsMilli - minScheduledStartTimeAsMilli) * compressionRatio);
        } catch (NoSuchElementException e) {
            // do nothing, just means stream was empty
        }

        for (int i = 0; i < peekingBlockingDependencyOperationStreams.size(); i++) {
            try {
                long firstAsMilli = peekingBlockingDependencyOperationStreams.get(i).peek().scheduledStartTimeAsMilli();
                peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.set(
                        i,
                        Math.round((firstAsMilli - minScheduledStartTimeAsMilli) * compressionRatio)
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
                        Math.round((firstAsMilli - minScheduledStartTimeAsMilli) * compressionRatio)
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
                originalWorkloadStreams.asynchronousStream().dependencyOperationTypes(),
                gf.timeOffsetAndCompress(
                        peekingAsyncDependencyOperationStream,
                        newStartTimeAsMilli + peekingAsyncDependencyOperationStreamAheadOfMinByAsMilli,
                        compressionRatio
                ),
                gf.timeOffsetAndCompress(
                        peekingAsyncNonDependencyOperationStream,
                        newStartTimeAsMilli + peekingAsyncNonDependencyOperationStreamAheadOfMinByAsMilli,
                        compressionRatio
                ),
                originalWorkloadStreams.asynchronousStream().childOperationGenerator()
        );

        for (int i = 0; i < blockingStreams.size(); i++) {
            timeOffsetAndCompressedWorkloadStreams.addBlockingStream(
                    blockingStreams.get(i).dependentOperationTypes(),
                    blockingStreams.get(i).dependencyOperationTypes(),
                    gf.timeOffsetAndCompress(
                            peekingBlockingDependencyOperationStreams.get(i),
                            newStartTimeAsMilli + peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.get(i),
                            compressionRatio
                    ),
                    gf.timeOffsetAndCompress(
                            peekingBlockingNonDependencyOperationStreams.get(i),
                            newStartTimeAsMilli + peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis.get(i),
                            compressionRatio
                    ),
                    blockingStreams.get(i).childOperationGenerator()
            );
        }

        return timeOffsetAndCompressedWorkloadStreams;
    }

    // returns (workload_streams, workload, minimum_timestamp)
    public static Tuple.Tuple3<WorkloadStreams, Workload, Long> createNewWorkloadWithLimitedWorkloadStreams(DriverConfiguration configuration,
                                                                                                            GeneratorFactory gf) throws WorkloadException, IOException {
        ClassNameWorkloadFactory workloadFactory = new ClassNameWorkloadFactory(configuration.workloadClassName());
        return createNewWorkloadWithLimitedWorkloadStreams(workloadFactory, configuration, gf);
    }

    // returns (workload_streams, workload, minimum_timestamp)
    public static Tuple.Tuple3<WorkloadStreams, Workload, Long> createNewWorkloadWithLimitedWorkloadStreams(WorkloadFactory workloadFactory,
                                                                                                            DriverConfiguration configuration,
                                                                                                            GeneratorFactory gf) throws WorkloadException, IOException {
        WorkloadStreams workloadStreams = new WorkloadStreams();
        // get workload
        Workload workload = workloadFactory.createWorkload();
        workload.init(configuration);
        // retrieve unbounded streams
//        // TODO check
        boolean hasDbConnected;
        WorkloadStreams unlimitedWorkloadStreams = workload.streams(gf, hasDbConnected = false);
        List<Iterator<Operation<?>>> streams = new ArrayList<>();
        streams.add(unlimitedWorkloadStreams.asynchronousStream().dependencyOperations());
        streams.add(unlimitedWorkloadStreams.asynchronousStream().nonDependencyOperations());
        for (WorkloadStreamDefinition stream : unlimitedWorkloadStreams.blockingStreamDefinitions()) {
            streams.add(stream.dependencyOperations());
            streams.add(stream.nonDependencyOperations());
        }
        // stream through streams once, to calculate how many operations are needed from each, to get operation_count in total
        Tuple.Tuple3<long[], Long, Long> limitsAndMinimumsForStream = WorkloadStreams.fromAmongAllRetrieveTopK(streams, configuration.operationCount());
        long[] limitForStream = limitsAndMinimumsForStream._1();
        long minimumDependencyTimeStamp = limitsAndMinimumsForStream._2();
        long minimumTimeStamp = limitsAndMinimumsForStream._3();

        workload.close();
        // reinitialize workload, so it can be streamed through from the beginning
        workload = workloadFactory.createWorkload();
        workload.init(configuration);
        // retrieve unbounded streams
        // TODO check
        unlimitedWorkloadStreams = workload.streams(gf, hasDbConnected = true);
        // copy unbounded streams to new workload streams instance, applying limits we just computed
        workloadStreams.setAsynchronousStream(
                unlimitedWorkloadStreams.asynchronousStream().dependentOperationTypes(),
                unlimitedWorkloadStreams.asynchronousStream().dependencyOperationTypes(),
                gf.limit(unlimitedWorkloadStreams.asynchronousStream().dependencyOperations(), limitForStream[0]),
                gf.limit(unlimitedWorkloadStreams.asynchronousStream().nonDependencyOperations(), limitForStream[1]),
                unlimitedWorkloadStreams.asynchronousStream().childOperationGenerator()
        );
        List<WorkloadStreamDefinition> blockingStreams = unlimitedWorkloadStreams.blockingStreamDefinitions();
        for (int i = 0; i < blockingStreams.size(); i++) {
            workloadStreams.addBlockingStream(
                    blockingStreams.get(i).dependentOperationTypes(),
                    blockingStreams.get(i).dependencyOperationTypes(),
                    gf.limit(blockingStreams.get(i).dependencyOperations(), limitForStream[i * 2 + 2]),
                    gf.limit(blockingStreams.get(i).nonDependencyOperations(), limitForStream[i * 2 + 3]),
                    blockingStreams.get(i).childOperationGenerator()
            );
        }
        return Tuple.tuple3(workloadStreams, workload, minimumTimeStamp);
    }

    // returns (limit_per_stream, minimum_dependency_timestamp, minimum_timestamp)
    public static Tuple.Tuple3<long[], Long, Long> fromAmongAllRetrieveTopK(List<Iterator<Operation<?>>> streams, long k) throws WorkloadException {
        final DecimalFormat numberFormat = new DecimalFormat("###,###,###,###,###");
        long minimumDependencyTimeStamp = Long.MAX_VALUE;
        long minimumTimeStamp = Long.MAX_VALUE;
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
            long minAsMilli = Long.MAX_VALUE;
            int indexOfMin = -1;
            for (int i = 0; i < streams.size(); i++) {
                if (null != streamHeads[i] || streams.get(i).hasNext()) {
                    if (null == streamHeads[i]) {
                        streamHeads[i] = streams.get(i).next();
                    }

                    long streamHeadTimeStampAsMilli = streamHeads[i].timeStamp();
                    long streamHeadDependencyTimeStampAsMilli = streamHeads[i].dependencyTimeStamp();

                    if (-1 == streamHeadTimeStampAsMilli)
                        throw new WorkloadException(String.format("Operation must have time stamp\n%s", streamHeads[i]));
                    if (-1 == streamHeadDependencyTimeStampAsMilli)
                        throw new WorkloadException(String.format("Operation must have dependency time stamp\n%s", streamHeads[i]));

                    if (streamHeadTimeStampAsMilli < minimumTimeStamp)
                        minimumTimeStamp = streamHeadTimeStampAsMilli;
                    if (streamHeadDependencyTimeStampAsMilli < minimumDependencyTimeStamp)
                        minimumDependencyTimeStamp = streamHeadDependencyTimeStampAsMilli;

                    if (null != streamHeads[i] && streamHeadTimeStampAsMilli < minAsMilli) {
                        minAsMilli = streamHeadTimeStampAsMilli;
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

            if (kSoFar % 1000000 == 0)
                System.out.print(String.format("Scanned %s of %s\r", numberFormat.format(kSoFar), numberFormat.format(k)));
        }

        return Tuple.tuple3(
                kForStream,
                minimumDependencyTimeStamp,
                minimumTimeStamp
        );
    }

    public WorkloadStreamDefinition asynchronousStream() {
        if (null != asynchronousStream) {
            return asynchronousStream;
        } else {
            return new WorkloadStreamDefinition(
                    new HashSet<Class<? extends Operation<?>>>(),
                    new HashSet<Class<? extends Operation<?>>>(),
                    Collections.<Operation<?>>emptyIterator(),
                    Collections.<Operation<?>>emptyIterator(),
                    null
            );
        }
    }

    public void setAsynchronousStream(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                      Set<Class<? extends Operation<?>>> dependencyOperationTypes,
                                      Iterator<Operation<?>> dependencyOperations,
                                      Iterator<Operation<?>> nonDependencyOperations,
                                      ChildOperationGenerator childOperationGenerator) {
        this.asynchronousStream = new WorkloadStreamDefinition(
                dependentOperationTypes,
                dependencyOperationTypes,
                dependencyOperations,
                nonDependencyOperations,
                childOperationGenerator
        );
    }

    public List<WorkloadStreamDefinition> blockingStreamDefinitions() {
        return blockingStreams;
    }

    public void addBlockingStream(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                  Set<Class<? extends Operation<?>>> dependencyOperationTypes,
                                  Iterator<Operation<?>> dependencyOperations,
                                  Iterator<Operation<?>> nonDependencyOperations,
                                  ChildOperationGenerator childOperationGenerator) {
        WorkloadStreamDefinition blockingStream = new WorkloadStreamDefinition(
                dependentOperationTypes,
                dependencyOperationTypes,
                dependencyOperations,
                nonDependencyOperations,
                childOperationGenerator
        );
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
        return gf.mergeSortOperationsByTimeStamp(allStreams.toArray(new Iterator[allStreams.size()]));
    }

    public static class WorkloadStreamDefinition {
        private final Set<Class<? extends Operation<?>>> dependentOperationTypes;
        private final Set<Class<? extends Operation<?>>> dependencyOperationTypes;
        private final Iterator<Operation<?>> dependencyOperations;
        private final Iterator<Operation<?>> nonDependencyOperations;
        private final ChildOperationGenerator childOperationGenerator;

        public WorkloadStreamDefinition(Set<Class<? extends Operation<?>>> dependentOperationTypes,
                                        Set<Class<? extends Operation<?>>> dependencyOperationTypes,
                                        Iterator<Operation<?>> dependencyOperations,
                                        Iterator<Operation<?>> nonDependencyOperations,
                                        ChildOperationGenerator childOperationGenerator) {
            this.dependentOperationTypes = dependentOperationTypes;
            this.dependencyOperationTypes = dependencyOperationTypes;
            this.dependencyOperations = dependencyOperations;
            this.nonDependencyOperations = nonDependencyOperations;
            this.childOperationGenerator = childOperationGenerator;
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

        public Set<Class<? extends Operation<?>>> dependencyOperationTypes() {
            return (null != dependencyOperationTypes) ? dependencyOperationTypes : new HashSet<Class<? extends Operation<?>>>();
        }

        public ChildOperationGenerator childOperationGenerator() {
            return childOperationGenerator;
        }
    }
}
