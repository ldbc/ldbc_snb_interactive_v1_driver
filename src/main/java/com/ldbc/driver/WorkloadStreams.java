package com.ldbc.driver;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple3;
import com.ldbc.driver.workloads.ClassNameWorkloadFactory;
import com.ldbc.driver.workloads.WorkloadFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static java.lang.String.format;

public class WorkloadStreams
{
    private WorkloadStreamDefinition asynchronousStream = null;
    private List<WorkloadStreamDefinition> blockingStreams = new ArrayList<>();

    public static WorkloadStreams timeOffsetAndCompressWorkloadStreams(
            WorkloadStreams originalWorkloadStreams,
            long newStartTimeAsMilli,
            double compressionRatio,
            GeneratorFactory gf ) throws WorkloadException
    {
        long minScheduledStartTimeAsMilli = Long.MAX_VALUE;

        /*
         * Find earliest scheduled start time from across all streams
         */

        PeekingIterator<Operation> peekingAsyncDependencyOperationStream =
                Iterators.peekingIterator( originalWorkloadStreams.asynchronousStream().dependencyOperations() );
        try
        {
            long firstAsMilli = peekingAsyncDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            if ( firstAsMilli < minScheduledStartTimeAsMilli )
            {
                minScheduledStartTimeAsMilli = firstAsMilli;
            }
        }
        catch ( NoSuchElementException e )
        {
            // do nothing, just means stream was empty
        }

        PeekingIterator<Operation> peekingAsyncNonDependencyOperationStream =
                Iterators.peekingIterator( originalWorkloadStreams.asynchronousStream().nonDependencyOperations() );
        try
        {
            long firstAsMilli = peekingAsyncNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            if ( firstAsMilli < minScheduledStartTimeAsMilli )
            {
                minScheduledStartTimeAsMilli = firstAsMilli;
            }
        }
        catch ( NoSuchElementException e )
        {
            // do nothing, just means stream was empty
        }

        List<Long> peekingBlockingDependencyOperationStreamsAheadOfMinByMillis = new ArrayList<>();
        List<PeekingIterator<Operation>> peekingBlockingDependencyOperationStreams = new ArrayList<>();
        List<Long> peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis = new ArrayList<>();
        List<PeekingIterator<Operation>> peekingBlockingNonDependencyOperationStreams = new ArrayList<>();
        List<WorkloadStreamDefinition> blockingStreams = originalWorkloadStreams.blockingStreamDefinitions();
        for ( int i = 0; i < blockingStreams.size(); i++ )
        {
            PeekingIterator<Operation> peekingBlockingDependencyOperationStream =
                    Iterators.peekingIterator( blockingStreams.get( i ).dependencyOperations() );
            try
            {
                long firstAsMilli = peekingBlockingDependencyOperationStream.peek().scheduledStartTimeAsMilli();
                if ( firstAsMilli < minScheduledStartTimeAsMilli )
                {
                    minScheduledStartTimeAsMilli = firstAsMilli;
                }
            }
            catch ( NoSuchElementException e )
            {
                // do nothing, just means stream was empty
            }
            peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.add( 0l );
            peekingBlockingDependencyOperationStreams.add( peekingBlockingDependencyOperationStream );

            PeekingIterator<Operation> peekingBlockingNonDependencyOperationStream =
                    Iterators.peekingIterator( blockingStreams.get( i ).nonDependencyOperations() );
            try
            {
                long firstAsMilli = peekingBlockingNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
                if ( firstAsMilli < minScheduledStartTimeAsMilli )
                {
                    minScheduledStartTimeAsMilli = firstAsMilli;
                }
            }
            catch ( NoSuchElementException e )
            {
                // do nothing, just means stream was empty
            }
            peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis.add( 0l );
            peekingBlockingNonDependencyOperationStreams.add( peekingBlockingNonDependencyOperationStream );
        }

        if ( Long.MAX_VALUE == minScheduledStartTimeAsMilli )
        {
            minScheduledStartTimeAsMilli = newStartTimeAsMilli;
        }

        /*
         * Find how far ahead of earliest scheduled start time each stream is when it starts
         */

        long peekingAsyncDependencyOperationStreamAheadOfMinByAsMilli = 0;
        try
        {
            long firstAsMilli = peekingAsyncDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            peekingAsyncDependencyOperationStreamAheadOfMinByAsMilli =
                    Math.round( (firstAsMilli - minScheduledStartTimeAsMilli) * compressionRatio );
        }
        catch ( NoSuchElementException e )
        {
            // do nothing, just means stream was empty
        }

        long peekingAsyncNonDependencyOperationStreamAheadOfMinByAsMilli = 0l;
        try
        {
            long firstAsMilli = peekingAsyncNonDependencyOperationStream.peek().scheduledStartTimeAsMilli();
            peekingAsyncNonDependencyOperationStreamAheadOfMinByAsMilli =
                    Math.round( (firstAsMilli - minScheduledStartTimeAsMilli) * compressionRatio );
        }
        catch ( NoSuchElementException e )
        {
            // do nothing, just means stream was empty
        }

        for ( int i = 0; i < peekingBlockingDependencyOperationStreams.size(); i++ )
        {
            try
            {
                long firstAsMilli =
                        peekingBlockingDependencyOperationStreams.get( i ).peek().scheduledStartTimeAsMilli();
                peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.set(
                        i,
                        Math.round( (firstAsMilli - minScheduledStartTimeAsMilli) * compressionRatio )
                );
            }
            catch ( NoSuchElementException e )
            {
                // do nothing, just means stream was empty
            }
        }

        for ( int i = 0; i < peekingBlockingNonDependencyOperationStreams.size(); i++ )
        {
            try
            {
                long firstAsMilli =
                        peekingBlockingNonDependencyOperationStreams.get( i ).peek().scheduledStartTimeAsMilli();
                peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis.set(
                        i,
                        Math.round( (firstAsMilli - minScheduledStartTimeAsMilli) * compressionRatio )
                );
            }
            catch ( NoSuchElementException e )
            {
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

        for ( int i = 0; i < blockingStreams.size(); i++ )
        {
            timeOffsetAndCompressedWorkloadStreams.addBlockingStream(
                    blockingStreams.get( i ).dependentOperationTypes(),
                    blockingStreams.get( i ).dependencyOperationTypes(),
                    gf.timeOffsetAndCompress(
                            peekingBlockingDependencyOperationStreams.get( i ),
                            newStartTimeAsMilli + peekingBlockingDependencyOperationStreamsAheadOfMinByMillis.get( i ),
                            compressionRatio
                    ),
                    gf.timeOffsetAndCompress(
                            peekingBlockingNonDependencyOperationStreams.get( i ),
                            newStartTimeAsMilli +
                            peekingBlockingNonDependencyOperationStreamsAheadOfMinByMillis.get( i ),
                            compressionRatio
                    ),
                    blockingStreams.get( i ).childOperationGenerator()
            );
        }

        return timeOffsetAndCompressedWorkloadStreams;
    }

    // returns (workload_streams, workload, minimum_timestamp)
    public static Tuple3<WorkloadStreams,Workload,Long> createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
            DriverConfiguration configuration,
            GeneratorFactory gf,
            boolean returnStreamsWithDbConnector,
            long offset,
            long limit,
            LoggingServiceFactory loggingServiceFactory ) throws WorkloadException, IOException
    {
        ClassNameWorkloadFactory workloadFactory = new ClassNameWorkloadFactory( configuration.workloadClassName() );
        return createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                workloadFactory,
                configuration,
                gf,
                returnStreamsWithDbConnector,
                offset,
                limit,
                loggingServiceFactory
        );
    }

    // returns (workload_streams, workload, minimum_timestamp)
    public static Tuple3<WorkloadStreams,Workload,Long> createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
            WorkloadFactory workloadFactory,
            DriverConfiguration configuration,
            GeneratorFactory gf,
            boolean returnStreamsWithDbConnector,
            long offset,
            long limit,
            LoggingServiceFactory loggingServiceFactory ) throws WorkloadException, IOException
    {
        // ================================
        // ====== Calculate Limits ========
        // ================================

        // get workload
        Workload workload = workloadFactory.createWorkload();
        workload.init( configuration );
        // retrieve unbounded streams
        boolean hasDbConnected = false;
        WorkloadStreams unlimitedWorkloadStreams = workload.streams( gf, hasDbConnected );
        List<Iterator<Operation>> streams = new ArrayList<>();
        List<ChildOperationGenerator> childOperationGenerators = new ArrayList<>();

        streams.add( unlimitedWorkloadStreams.asynchronousStream().dependencyOperations() );
        childOperationGenerators.add( unlimitedWorkloadStreams.asynchronousStream().childOperationGenerator() );

        streams.add( unlimitedWorkloadStreams.asynchronousStream().nonDependencyOperations() );
        childOperationGenerators.add( unlimitedWorkloadStreams.asynchronousStream().childOperationGenerator() );

        for ( WorkloadStreamDefinition stream : unlimitedWorkloadStreams.blockingStreamDefinitions() )
        {
            streams.add( stream.dependencyOperations() );
            childOperationGenerators.add( stream.childOperationGenerator() );

            streams.add( stream.nonDependencyOperations() );
            childOperationGenerators.add( stream.childOperationGenerator() );
        }

        // stream through streams once, to calculate how many operations are needed from each,
        // to get operation_count in total
        Tuple3<long[],long[],Long> limitsAndMinimumsForStream =
                WorkloadStreams.fromAmongAllRetrieveTopCountFromOffset(
                        streams,
                        offset,
                        limit,
                        childOperationGenerators,
                        loggingServiceFactory
                );
        long[] startForStream = limitsAndMinimumsForStream._1();
        long[] limitForStream = limitsAndMinimumsForStream._2();
        long minimumTimeStamp = limitsAndMinimumsForStream._3();

        workload.close();

        // ================================
        // ====== Create Limited Streams ==
        // ================================

        WorkloadStreams workloadStreams = new WorkloadStreams();

        // reinitialize workload, so it can be streamed through from the beginning
        workload = workloadFactory.createWorkload();
        workload.init( configuration );

        // retrieve unbounded streams
        unlimitedWorkloadStreams = workload.streams( gf, returnStreamsWithDbConnector );
        List<WorkloadStreamDefinition> unlimitedBlockingStreams = unlimitedWorkloadStreams.blockingStreamDefinitions();

        // advance to offsets
        gf.consume( unlimitedWorkloadStreams.asynchronousStream().dependencyOperations(), startForStream[0] );
        gf.consume( unlimitedWorkloadStreams.asynchronousStream().nonDependencyOperations(), startForStream[1] );
        for ( int i = 0; i < unlimitedBlockingStreams.size(); i++ )
        {
            gf.consume( unlimitedBlockingStreams.get( i ).dependencyOperations(), startForStream[i * 2 + 2] );
            gf.consume( unlimitedBlockingStreams.get( i ).nonDependencyOperations(), startForStream[i * 2 + 3] );
        }

        // copy unbounded streams to new workload streams instance, from offsets, applying limits
        workloadStreams.setAsynchronousStream(
                unlimitedWorkloadStreams.asynchronousStream().dependentOperationTypes(),
                unlimitedWorkloadStreams.asynchronousStream().dependencyOperationTypes(),
                gf.limit( unlimitedWorkloadStreams.asynchronousStream().dependencyOperations(), limitForStream[0] ),
                gf.limit( unlimitedWorkloadStreams.asynchronousStream().nonDependencyOperations(), limitForStream[1] ),
                unlimitedWorkloadStreams.asynchronousStream().childOperationGenerator()
        );
        for ( int i = 0; i < unlimitedBlockingStreams.size(); i++ )
        {
            workloadStreams.addBlockingStream(
                    unlimitedBlockingStreams.get( i ).dependentOperationTypes(),
                    unlimitedBlockingStreams.get( i ).dependencyOperationTypes(),
                    gf.limit( unlimitedBlockingStreams.get( i ).dependencyOperations(), limitForStream[i * 2 + 2] ),
                    gf.limit( unlimitedBlockingStreams.get( i ).nonDependencyOperations(), limitForStream[i * 2 + 3] ),
                    unlimitedBlockingStreams.get( i ).childOperationGenerator()
            );
        }

        return Tuple.tuple3(
                workloadStreams,
                workload,
                minimumTimeStamp
        );
    }

    // returns (start_per_stream, end_per_stream, minimum_timestamp)
    public static Tuple3<long[],long[],Long> fromAmongAllRetrieveTopCountFromOffset(
            List<Iterator<Operation>> streams,
            long offset,
            long limit,
            List<ChildOperationGenerator> childOperationGenerators,
            LoggingServiceFactory loggingServiceFactory ) throws WorkloadException
    {
        LoggingService loggingService =
                loggingServiceFactory.loggingServiceFor( WorkloadStreams.class.getSimpleName() );
        final DecimalFormat numberFormat = new DecimalFormat( "###,###,###,###,###" );
        final Object result = null;
        Operation operation;
        ChildOperationGenerator childOperationGenerator;
        // last operation retrieved (which has not yet been counted) from each stream
        Operation[] streamHeads = new Operation[streams.size()];
        for ( int i = 0; i < streams.size(); i++ )
        {
            streamHeads[i] = null;
        }

        // ================================================
        // ===== advance to start point of each stream =====
        // ================================================

        // count of operations to retrieve from that particular stream
        long[] kForStreamOffset = new long[streams.size()];
        for ( int i = 0; i < streams.size(); i++ )
        {
            kForStreamOffset[i] = 0;
        }
        long kSoFarOffset = 0;

        while ( kSoFarOffset < offset )
        {
            long minAsMilli = Long.MAX_VALUE;
            int indexOfMin = -1;
            for ( int i = 0; i < streams.size(); i++ )
            {
                if ( null != streamHeads[i] || streams.get( i ).hasNext() )
                {
                    if ( null == streamHeads[i] )
                    {
                        streamHeads[i] = streams.get( i ).next();
                    }

                    long streamHeadTimeStampAsMilli = streamHeads[i].timeStamp();

                    if ( -1 == streamHeadTimeStampAsMilli )
                    {
                        throw new WorkloadException(
                                format( "Operation must have time stamp\n%s", streamHeads[i] ) );
                    }

                    if ( -1 == streamHeads[i].dependencyTimeStamp() )
                    {
                        throw new WorkloadException(
                                format( "Operation must have dependency time stamp\n%s", streamHeads[i] ) );
                    }

                    if ( null != streamHeads[i] && streamHeadTimeStampAsMilli < minAsMilli )
                    {
                        minAsMilli = streamHeadTimeStampAsMilli;
                        indexOfMin = i;
                    }
                }
            }
            if ( -1 == indexOfMin )
            {
                // iterators are empty, nothing left to retrieve
                break;
            }
            kForStreamOffset[indexOfMin] = kForStreamOffset[indexOfMin] + 1;
            kSoFarOffset = kSoFarOffset + 1;

            operation = streamHeads[indexOfMin];
            childOperationGenerator = childOperationGenerators.get( indexOfMin );
            if ( null != childOperationGenerator )
            {
                double state = childOperationGenerator.initialState();
                while ( null != (operation = childOperationGenerator
                        .nextOperation( state, operation, result, operation.scheduledStartTimeAsMilli(), 0l )) )
                {
                    kSoFarOffset = kSoFarOffset + 1;
                    state = childOperationGenerator.updateState( state, operation.type() );
                }
            }

            streamHeads[indexOfMin] = null;

            if ( kSoFarOffset % 1000000 == 0 )
            {
                loggingService.info(
                        format(
                                "Scanned %s of %s - OFFSET\r",
                                numberFormat.format( kSoFarOffset ),
                                numberFormat.format( offset )
                        )
                );
            }
        }
        loggingService.info(
                format(
                        "Scanned %s of %s - OFFSET",
                        numberFormat.format( kSoFarOffset ),
                        numberFormat.format( offset )
                )
        );

        // ================================================
        // ===== calculate end points for each stream =====
        // ================================================

        long minimumTimeStamp = Long.MAX_VALUE;
        // count of operations to retrieve from that particular stream
        long[] kForStreamRun = new long[streams.size()];
        for ( int i = 0; i < streams.size(); i++ )
        {
            kForStreamRun[i] = 0;
        }
        long kSoFarRun = 0;

        while ( kSoFarRun < limit )
        {
            long minAsMilli = Long.MAX_VALUE;
            int indexOfMin = -1;
            for ( int i = 0; i < streams.size(); i++ )
            {
                if ( null != streamHeads[i] || streams.get( i ).hasNext() )
                {
                    if ( null == streamHeads[i] )
                    {
                        streamHeads[i] = streams.get( i ).next();
                    }

                    long streamHeadTimeStampAsMilli = streamHeads[i].timeStamp();
                    long streamHeadDependencyTimeStampAsMilli = streamHeads[i].dependencyTimeStamp();

                    if ( -1 == streamHeadTimeStampAsMilli )
                    {
                        throw new WorkloadException(
                                format( "Operation must have time stamp\n%s", streamHeads[i] ) );
                    }

                    if ( -1 == streamHeadDependencyTimeStampAsMilli )
                    {
                        throw new WorkloadException(
                                format( "Operation must have dependency time stamp\n%s", streamHeads[i] ) );
                    }

                    if ( streamHeadTimeStampAsMilli < minimumTimeStamp )
                    {
                        minimumTimeStamp = streamHeadTimeStampAsMilli;
                    }

                    if ( null != streamHeads[i] && streamHeadTimeStampAsMilli < minAsMilli )
                    {
                        minAsMilli = streamHeadTimeStampAsMilli;
                        indexOfMin = i;
                    }
                }
            }
            if ( -1 == indexOfMin )
            {
                // iterators are empty, nothing left to retrieve
                break;
            }
            kForStreamRun[indexOfMin] = kForStreamRun[indexOfMin] + 1;
            kSoFarRun = kSoFarRun + 1;

            operation = streamHeads[indexOfMin];
            childOperationGenerator = childOperationGenerators.get( indexOfMin );
            if ( null != childOperationGenerator )
            {
                double state = childOperationGenerator.initialState();
                while ( null != (operation = childOperationGenerator
                        .nextOperation( state, operation, result, operation.scheduledStartTimeAsMilli(), 0l )) )
                {
                    kSoFarRun = kSoFarRun + 1;
                    state = childOperationGenerator.updateState( state, operation.type() );
                }
            }

            streamHeads[indexOfMin] = null;

            if ( kSoFarRun % 1000000 == 0 )
            {
                loggingService.info(
                        format( "Scanned %s of %s - RUN\r",
                                numberFormat.format( kSoFarRun ),
                                numberFormat.format( limit )
                        )
                );
            }
        }
        loggingService.info(
                format(
                        "Scanned %s of %s - RUN",
                        numberFormat.format( kSoFarRun ),
                        numberFormat.format( limit )
                )
        );

        return Tuple.tuple3(
                kForStreamOffset,
                kForStreamRun,
                minimumTimeStamp
        );
    }

    public WorkloadStreamDefinition asynchronousStream()
    {
        if ( null != asynchronousStream )
        {
            return asynchronousStream;
        }
        else
        {
            return new WorkloadStreamDefinition(
                    new HashSet<Class<? extends Operation>>(),
                    new HashSet<Class<? extends Operation>>(),
                    Collections.<Operation>emptyIterator(),
                    Collections.<Operation>emptyIterator(),
                    null
            );
        }
    }

    public void setAsynchronousStream( Set<Class<? extends Operation>> dependentOperationTypes,
            Set<Class<? extends Operation>> dependencyOperationTypes,
            Iterator<Operation> dependencyOperations,
            Iterator<Operation> nonDependencyOperations,
            ChildOperationGenerator childOperationGenerator )
    {
        this.asynchronousStream = new WorkloadStreamDefinition(
                dependentOperationTypes,
                dependencyOperationTypes,
                dependencyOperations,
                nonDependencyOperations,
                childOperationGenerator
        );
    }

    public List<WorkloadStreamDefinition> blockingStreamDefinitions()
    {
        return blockingStreams;
    }

    public void addBlockingStream( Set<Class<? extends Operation>> dependentOperationTypes,
            Set<Class<? extends Operation>> dependencyOperationTypes,
            Iterator<Operation> dependencyOperations,
            Iterator<Operation> nonDependencyOperations,
            ChildOperationGenerator childOperationGenerator )
    {
        WorkloadStreamDefinition blockingStream = new WorkloadStreamDefinition(
                dependentOperationTypes,
                dependencyOperationTypes,
                dependencyOperations,
                nonDependencyOperations,
                childOperationGenerator
        );
        this.blockingStreams.add( blockingStream );
    }

    public static Iterator<Operation> mergeSortedByStartTimeExcludingChildOperationGenerators( GeneratorFactory gf,
            WorkloadStreams workloadStreams )
    {
        List<Iterator<Operation>> allStreams = new ArrayList<>();
        for ( WorkloadStreamDefinition streamDefinition : workloadStreams.blockingStreamDefinitions() )
        {
            allStreams.add( streamDefinition.dependencyOperations() );
            allStreams.add( streamDefinition.nonDependencyOperations() );
        }
        allStreams.add( workloadStreams.asynchronousStream().dependencyOperations() );
        allStreams.add( workloadStreams.asynchronousStream().nonDependencyOperations() );
        return gf.mergeSortOperationsByTimeStamp( allStreams.toArray( new Iterator[allStreams.size()] ) );
    }

    public static class WorkloadStreamDefinition
    {
        private final Set<Class<? extends Operation>> dependentOperationTypes;
        private final Set<Class<? extends Operation>> dependencyOperationTypes;
        private final Iterator<Operation> dependencyOperations;
        private final Iterator<Operation> nonDependencyOperations;
        private final ChildOperationGenerator childOperationGenerator;

        public WorkloadStreamDefinition( Set<Class<? extends Operation>> dependentOperationTypes,
                Set<Class<? extends Operation>> dependencyOperationTypes,
                Iterator<Operation> dependencyOperations,
                Iterator<Operation> nonDependencyOperations,
                ChildOperationGenerator childOperationGenerator )
        {
            this.dependentOperationTypes = dependentOperationTypes;
            this.dependencyOperationTypes = dependencyOperationTypes;
            this.dependencyOperations = dependencyOperations;
            this.nonDependencyOperations = nonDependencyOperations;
            this.childOperationGenerator = childOperationGenerator;
        }

        public Iterator<Operation> dependencyOperations()
        {
            return (null != dependencyOperations) ? dependencyOperations : Collections.<Operation>emptyIterator();
        }

        public Iterator<Operation> nonDependencyOperations()
        {
            return (null != nonDependencyOperations) ? nonDependencyOperations : Collections.<Operation>emptyIterator();
        }

        public Set<Class<? extends Operation>> dependentOperationTypes()
        {
            return (null != dependentOperationTypes) ? dependentOperationTypes
                                                     : new HashSet<Class<? extends Operation>>();
        }

        public Set<Class<? extends Operation>> dependencyOperationTypes()
        {
            return (null != dependencyOperationTypes) ? dependencyOperationTypes
                                                      : new HashSet<Class<? extends Operation>>();
        }

        public ChildOperationGenerator childOperationGenerator()
        {
            return childOperationGenerator;
        }
    }
}
