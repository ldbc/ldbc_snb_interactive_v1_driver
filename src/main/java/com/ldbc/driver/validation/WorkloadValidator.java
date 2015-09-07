package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.SerializingMarshallingException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Tuple3;
import com.ldbc.driver.workloads.WorkloadFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ldbc.driver.validation.WorkloadValidationResult.ResultType;
import static java.lang.String.format;

// TODO the below could be used as a guide for how to do this
// Synchronous      NONE    makesSense(y)    dependencyTime(n)  startTimeDependencyTimeDifference(n)
// Asynchronous     NONE    makesSense(y)    dependencyTime(n)  startTimeDependencyTimeDifference(n)
//
// Synchronous      READ    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=0)
// Asynchronous     READ    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=0)
//
// Synchronous      READ_WRITE    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=0)
// Asynchronous     READ_WRITE    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=0)

// TODO add test for ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_SCHEDULING_MODE
public class WorkloadValidator
{
    /**
     * @param workloadFactory
     * @param configuration
     * @return
     */
    public WorkloadValidationResult validate(
            WorkloadFactory workloadFactory,
            DriverConfiguration configuration,
            LoggingServiceFactory loggingServiceFactory )
    {
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) );
        TimeSource timeSource = new SystemTimeSource();
        long nowAsMilli = timeSource.nowAsMilli();
        long operationCount;

        /*
         * *************************************************************************************************************
         *   FIRST PHASE JUST CHECK THAT ALL OPERATIONS HAVE TIMES ASSIGNED
         * *************************************************************************************************************
         */
        Workload workloadPass1;
        Iterator<Operation> operationsPass1;
        try
        {
            long offset = 0;
            long limit = configuration.operationCount();
            boolean returnStreamsWithDbConnector = false;
            Tuple3<WorkloadStreams,Workload,Long> streamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            workloadFactory,
                            configuration,
                            gf,
                            returnStreamsWithDbConnector,
                            offset,
                            limit,
                            loggingServiceFactory
                    );
            operationsPass1 = WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                    gf,
                    streamsAndWorkload._1()
            );
            workloadPass1 = streamsAndWorkload._2();
        }
        catch ( Exception e )
        {
            return new WorkloadValidationResult(
                    ResultType.UNEXPECTED,
                    format( "Error retrieving operations from workload\n%s",
                            ConcurrentErrorReporter.stackTraceToString( e ) )
            );
        }

        operationCount = 0;
        while ( operationsPass1.hasNext() )
        {
            Operation operation = operationsPass1.next();
            operationCount++;

            // Operation has time stamp
            long operationTimeStampAsMilli = operation.timeStamp();
            if ( 0 > operationTimeStampAsMilli )
            {
                return new WorkloadValidationResult(
                        ResultType.UNASSIGNED_TIME_STAMP,
                        format( "Operation %s - Unassigned operation time stamp\n" +
                                "  --> %s",
                                operationCount,
                                operation ) );
            }

            // Operation has start time
            long operationStartTimeAsMilli = operation.scheduledStartTimeAsMilli();
            if ( 0 > operationStartTimeAsMilli )
            {
                return new WorkloadValidationResult(
                        ResultType.UNASSIGNED_SCHEDULED_START_TIME,
                        format( "Operation %s - Unassigned operation scheduled start time\n" +
                                "  --> %s",
                                operationCount,
                                operation ) );
            }

            long operationDependencyTimeStamp = operation.dependencyTimeStamp();
            // Operation has dependency time stamp
            if ( 0 > operationDependencyTimeStamp )
            {
                return new WorkloadValidationResult(
                        ResultType.UNASSIGNED_DEPENDENCY_TIME_STAMP,
                        format( "Operation %s - Unassigned operation dependency time stamp\n" +
                                "  --> Operation: %s",
                                operationCount,
                                operation ) );
            }

            // Ensure operation dependency time stamp is less than operation time stamp
            if ( operationDependencyTimeStamp >= operationStartTimeAsMilli )
            {
                return new WorkloadValidationResult(
                        ResultType.DEPENDENCY_TIME_STAMP_IS_NOT_BEFORE_TIME_STAMP,
                        format( ""
                                +
                                "Operation %s - Operation dependency time is not less than operation start time\n"
                                + "  Operation: %s\n"
                                + "  Time Stamp: %s\n"
                                + "  Start Time: %s\n"
                                + "  Dependency Time: %s",
                                operationCount,
                                operation,
                                operation.timeStamp(),
                                operation.scheduledStartTimeAsMilli(),
                                operation.dependencyTimeStamp() ) );
            }
        }

        try
        {
            workloadPass1.close();
        }
        catch ( IOException e )
        {
            return new WorkloadValidationResult(
                    ResultType.UNEXPECTED,
                    "Error during workload cleanup\n" + ConcurrentErrorReporter.stackTraceToString( e ) );
        }

        /*
         * *************************************************************************************************************
         *   SECOND PHASE PERFORM MORE ELABORATE CHECKS
         * *************************************************************************************************************
         */

        Workload workloadPass2;
        Iterator<Operation> operationsPass2;
        try
        {
            long offset = 0;
            long limit = configuration.operationCount();
            boolean returnStreamsWithDbConnector = false;
            Tuple3<WorkloadStreams,Workload,Long> streamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            workloadFactory,
                            configuration,
                            gf,
                            returnStreamsWithDbConnector,
                            offset,
                            limit,
                            loggingServiceFactory
                    );
            workloadPass2 = streamsAndWorkload._2();
            operationsPass2 = gf.timeOffsetAndCompress(
                    WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(
                            gf,
                            streamsAndWorkload._1()
                    ),
                    nowAsMilli,
                    configuration.timeCompressionRatio()
            );

        }
        catch ( Exception e )
        {
            return new WorkloadValidationResult( ResultType.UNEXPECTED,
                    format( "Error while retrieving operations from workload\n%s",
                            ConcurrentErrorReporter.stackTraceToString( e ) ) );
        }

        Operation previousOperation = null;
        long previousOperationTimeStampAsMilli = -1;
        long previousOperationStartTimeAsMilli = -1;

        Map<Class,Long> previousOperationStartTimesAsMilliByOperationType = new HashMap<>();
        Map<Class,ContinuousMetricManager> operationInterleavesByOperationType = new HashMap<>();

        operationCount = 0;
        while ( operationsPass2.hasNext() )
        {
            Operation operation = operationsPass2.next();
            Class operationType = operation.getClass();
            operationCount++;

            // Operation has start time
            long operationTimeStamp = operation.timeStamp();
            if ( 0 > operationTimeStamp )
            {
                return new WorkloadValidationResult(
                        ResultType.UNASSIGNED_TIME_STAMP,
                        format( "Operation %s - Unassigned operation time stamp\n" +
                                "  --> %s",
                                operationCount,
                                operation ) );
            }

            // Operation has start time
            long operationStartTimeAsMilli = operation.scheduledStartTimeAsMilli();
            if ( 0 > operationStartTimeAsMilli )
            {
                return new WorkloadValidationResult(
                        ResultType.UNASSIGNED_SCHEDULED_START_TIME,
                        format( "Operation %s - Unassigned operation scheduled start time\n  %s",
                                operationCount,
                                operation ) );
            }

            // Operation time stamps increase monotonically
            if ( -1 != previousOperationTimeStampAsMilli &&
                 operationTimeStamp < previousOperationTimeStampAsMilli )
            {
                return new WorkloadValidationResult(
                        ResultType.TIME_STAMPS_DO_NOT_INCREASE_MONOTONICALLY,
                        format( ""
                                + "Operation %s - Operation time stamps do not increase monotonically\n"
                                + "  Previous: %s\n"
                                + "  Current: %s",
                                operationCount,
                                previousOperation,
                                operation ) );
            }

            // Operation start times increase monotonically
            if ( -1 != previousOperationStartTimeAsMilli &&
                 operationStartTimeAsMilli < previousOperationStartTimeAsMilli )
            {
                return new WorkloadValidationResult(
                        ResultType.SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY,
                        format( ""
                                + "Operation %s - Operation start times do not increase monotonically\n"
                                + "  Previous: %s\n"
                                + "  Current: %s",
                                operationCount,
                                previousOperation,
                                operation ) );
            }

            // Interleaves do not exceed maximum
            if ( -1 != previousOperationStartTimeAsMilli )
            {
                long interleaveDurationAsMilli = operationStartTimeAsMilli - previousOperationStartTimeAsMilli;
                if ( interleaveDurationAsMilli > workloadPass2.maxExpectedInterleaveAsMilli() )
                {
                    return new WorkloadValidationResult(
                            ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM,
                            format( "Operation %s - encountered interleave exceeds maximum\n" +
                                    "  Interleave: %s (ms)/ %s\n" +
                                    "  Maximum: %s\n" +
                                    "  Previous: %s\n" +
                                    "  Current: %s",
                                    operationCount,
                                    interleaveDurationAsMilli,
                                    new TemporalUtil().milliDurationToString( interleaveDurationAsMilli ),
                                    workloadPass2.maxExpectedInterleaveAsMilli(),
                                    previousOperation,
                                    operation ) );
                }
            }

            long operationDependencyTimeStamp = operation.dependencyTimeStamp();

            // Operation has dependency time stamp
            if ( 0 > operationDependencyTimeStamp )
            {
                return new WorkloadValidationResult(
                        ResultType.UNASSIGNED_DEPENDENCY_TIME_STAMP,
                        format( "Operation %s - Unassigned operation dependency time stamp\n" +
                                "  --> Operation: %s",
                                operationCount,
                                operation ) );
            }

            // Ensure operation dependency time stamp is less than operation time stamp
            if ( operationDependencyTimeStamp >= operationTimeStamp )
            {
                return new WorkloadValidationResult(
                        ResultType.DEPENDENCY_TIME_STAMP_IS_NOT_BEFORE_TIME_STAMP,
                        format( "Operation %s - dependency time stamp is not less than operation time stamp\n"
                                + "  Operation: %s\n"
                                + "  Start Time: %s\n"
                                + "  Dependency Time: %s",
                                operationCount,
                                operation,
                                operation.timeStamp(),
                                operation.dependencyTimeStamp() ) );
            }

            // Interleaves by operation type do not exceed maximum
            ContinuousMetricManager operationInterleaveForOperationType =
                    operationInterleavesByOperationType.get( operationType );
            if ( null == operationInterleaveForOperationType )
            {
                operationInterleaveForOperationType =
                        new ContinuousMetricManager( null, null, workloadPass2.maxExpectedInterleaveAsMilli(), 5 );
                operationInterleavesByOperationType.put( operationType, operationInterleaveForOperationType );
            }
            long previousOperationStartTimeAsMilliByOperationType =
                    ((previousOperationStartTimesAsMilliByOperationType.containsKey( operationType )))
                    ? previousOperationStartTimesAsMilliByOperationType.get( operationType )
                    : -1;
            if ( -1 != previousOperationStartTimeAsMilliByOperationType )
            {
                long interleaveDurationAsMilli =
                        operationStartTimeAsMilli - previousOperationStartTimeAsMilliByOperationType;
                if ( interleaveDurationAsMilli > workloadPass2.maxExpectedInterleaveAsMilli() )
                {
                    return new WorkloadValidationResult(
                            ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_OPERATION_TYPE,
                            format( ""
                                    +
                                    "Operation %s - Encountered interleave duration (for %s) %s that exceeds " +
                                    "maximum expected value (%s)\n"
                                    + "  Previous: %s\n"
                                    + "  Current: %s",
                                    operationCount,
                                    operationType.getSimpleName(),
                                    interleaveDurationAsMilli,
                                    workloadPass2.maxExpectedInterleaveAsMilli(),
                                    previousOperationStartTimeAsMilliByOperationType,
                                    operationStartTimeAsMilli ) );
                }
            }

            // Serializing and Marshalling operations works
            String serializedOperation;
            try
            {
                serializedOperation = workloadPass2.serializeOperation( operation );
            }
            catch ( SerializingMarshallingException e )
            {
                return new WorkloadValidationResult(
                        ResultType.UNABLE_TO_SERIALIZE_OPERATION,
                        format( "Operation %s - Unable to serialize operation\nOperation: %s",
                                operationCount,
                                operation ) );
            }
            Operation marshaledOperation;
            try
            {
                marshaledOperation = workloadPass2.marshalOperation( serializedOperation );
            }
            catch ( SerializingMarshallingException e )
            {
                return new WorkloadValidationResult(
                        ResultType.UNABLE_TO_MARSHAL_OPERATION,
                        format( "Unable to marshal operation\nOperation: %s",
                                serializedOperation ) );
            }
            if ( false == operation.equals( marshaledOperation ) )
            {
                return new WorkloadValidationResult(
                        ResultType.OPERATIONS_DO_NOT_EQUAL_AFTER_SERIALIZING_AND_MARSHALLING,
                        format( ""
                                + "Operation %s - Operations do not equal after serializing and marshalling\n"
                                + "  Original Operation: %s\n"
                                + "  Serialized Operation: %s\n"
                                + "  Marshaled Operation: %s",
                                operationCount,
                                operation,
                                serializedOperation,
                                marshaledOperation ) );
            }

            previousOperation = operation;
            previousOperationStartTimeAsMilli = operationStartTimeAsMilli;
            previousOperationStartTimesAsMilliByOperationType.put( operationType, operationStartTimeAsMilli );
        }

        try
        {
            workloadPass2.close();
        }
        catch ( IOException e )
        {
            return new WorkloadValidationResult(
                    ResultType.UNEXPECTED,
                    "Error during workload cleanup\n" + ConcurrentErrorReporter.stackTraceToString( e ) );
        }

        /*
         * *************************************************************************************************************
         *   THIRD PHASE PERFORM DETERMINISM CHECK
         * *************************************************************************************************************
         */
        Workload workload1;
        Workload workload2;
        Iterator<Operation> operationStream1;
        Iterator<Operation> operationStream2;
        try
        {
            boolean returnStreamsWithDbConnector = false;
            Tuple3<WorkloadStreams,Workload,Long> streamsAndWorkload1 =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            workloadFactory,
                            configuration,
                            new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) ),
                            returnStreamsWithDbConnector,
                            0,
                            configuration.operationCount(),
                            loggingServiceFactory
                    );
            workload1 = streamsAndWorkload1._2();
            operationStream1 = gf.timeOffsetAndCompress(
                    WorkloadStreams
                            .mergeSortedByStartTimeExcludingChildOperationGenerators( gf, streamsAndWorkload1._1() ),
                    nowAsMilli,
                    configuration.timeCompressionRatio()
            );

            Tuple3<WorkloadStreams,Workload,Long> streamsAndWorkload2 =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            workloadFactory,
                            configuration,
                            new GeneratorFactory( new RandomDataGeneratorFactory( 42l ) ),
                            returnStreamsWithDbConnector,
                            0,
                            configuration.operationCount(),
                            loggingServiceFactory
                    );
            workload2 = streamsAndWorkload2._2();
            operationStream2 = gf.timeOffsetAndCompress(
                    WorkloadStreams
                            .mergeSortedByStartTimeExcludingChildOperationGenerators( gf, streamsAndWorkload2._1() ),
                    nowAsMilli,
                    configuration.timeCompressionRatio()
            );
        }
        catch ( Exception e )
        {
            return new WorkloadValidationResult( ResultType.UNEXPECTED,
                    format( "Error while retrieving operations from workload\n%s",
                            ConcurrentErrorReporter.stackTraceToString( e ) ) );
        }

        try
        {
            boolean compareTimes = true;
            GeneratorFactory.OperationStreamComparisonResult operationStreamComparisonResult =
                    gf.compareOperationStreams( operationStream1, operationStream2, compareTimes );
            if ( GeneratorFactory.OperationStreamComparisonResultType.PASS !=
                 operationStreamComparisonResult.resultType() )
            {
                return new WorkloadValidationResult(
                        ResultType.WORKLOAD_IS_NOT_DETERMINISTIC,
                        "Workload is not deterministic\n" + operationStreamComparisonResult.errorMessage() );
            }
        }
        catch ( Exception e )
        {
            return new WorkloadValidationResult(
                    ResultType.UNEXPECTED,
                    format( "Unexpected error encountered while checking if workload is deterministic\n%s",
                            ConcurrentErrorReporter.stackTraceToString( e ) ) );
        }

        try
        {
            workload1.close();
            workload2.close();
        }
        catch ( IOException e )
        {
            return new WorkloadValidationResult(
                    ResultType.UNEXPECTED,
                    "Error during workload creation\n" + ConcurrentErrorReporter.stackTraceToString( e ) );
        }

        return new WorkloadValidationResult( ResultType.SUCCESSFUL, null );
    }
}
