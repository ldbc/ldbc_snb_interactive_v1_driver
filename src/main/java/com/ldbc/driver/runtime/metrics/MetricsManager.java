package com.ldbc.driver.runtime.metrics;

import com.google.common.collect.Ordering;
import com.ldbc.driver.Operation;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.temporal.TimeSource;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class MetricsManager
{
    private final TimeSource timeSource;
    private final TimeUnit unit;
    private long startTimeAsMilli;
    private long latestFinishTimeAsMilli;
    private final OperationTypeMetricsManager[] operationTypeMetricsManagers;

    public static void export(
            WorkloadResultsSnapshot workloadResults,
            WorkloadMetricsFormatter metricsFormatter,
            OutputStream outputStream,
            Charset charSet )
            throws MetricsCollectionException
    {
        try
        {
            String formattedMetricsGroups = metricsFormatter.format( workloadResults );
            outputStream.write( formattedMetricsGroups.getBytes( charSet ) );
        }
        catch ( Exception e )
        {
            throw new MetricsCollectionException( "Error encountered writing metrics to output stream", e );
        }
    }

    public static OperationTypeMetricsManager[] toOperationTypeMetricsManagerArray(
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            TimeUnit unit,
            long highestExpectedRuntimeDurationAsNano,
            LoggingServiceFactory loggingServiceFactory ) throws MetricsCollectionException
    {
        if ( operationTypeToClassMapping.isEmpty() )
        {
            return new OperationTypeMetricsManager[]{};
        }
        else
        {
            final int minOperationType = Ordering.<Integer>natural().min( operationTypeToClassMapping.keySet() );
            if ( minOperationType < 0 )
            {
                throw new MetricsCollectionException(
                        format( "Encountered Operation with type code lower than 0: %s", minOperationType ) );
            }

            final int maxOperationType = Ordering.<Integer>natural().max( operationTypeToClassMapping.keySet() );
            OperationTypeMetricsManager[] operationTypeMetricsManagers =
                    new OperationTypeMetricsManager[maxOperationType + 1];
            for ( int i = 0; i < operationTypeMetricsManagers.length; i++ )
            {
                Class<? extends Operation> operationClass = operationTypeToClassMapping.get( i );
                if ( null == operationClass )
                {
                    operationTypeMetricsManagers[i] = null;
                }
                else
                {
                    operationTypeMetricsManagers[i] = new OperationTypeMetricsManager(
                            operationClass.getSimpleName(),
                            unit,
                            highestExpectedRuntimeDurationAsNano,
                            loggingServiceFactory
                    );
                }
            }
            return operationTypeMetricsManagers;
        }
    }

    public static String[] toOperationNameArray( Map<Integer,Class<? extends Operation>> operationTypeToClassMapping )
            throws MetricsCollectionException
    {
        if ( operationTypeToClassMapping.isEmpty() )
        {
            return new String[]{};
        }
        else
        {
            final int minOperationType = Ordering.<Integer>natural().min( operationTypeToClassMapping.keySet() );
            if ( minOperationType < 0 )
            {
                throw new MetricsCollectionException(
                        format( "Encountered Operation with type code lower than 0: %s", minOperationType ) );
            }
            final int maxOperationType = Ordering.<Integer>natural().max( operationTypeToClassMapping.keySet() );
            String[] operationNames = new String[maxOperationType + 1];
            for ( int i = 0; i < operationNames.length; i++ )
            {
                Class<? extends Operation> operationClass = operationTypeToClassMapping.get( i );
                if ( null == operationClass )
                {
                    operationNames[i] = null;
                }
                else
                {
                    operationNames[i] = operationClass.getSimpleName();
                }
            }
            return operationNames;
        }
    }

    MetricsManager( TimeSource timeSource,
            TimeUnit unit,
            long highestExpectedRuntimeDurationAsNano,
            Map<Integer,Class<? extends Operation>> operationTypeToClassMapping,
            LoggingServiceFactory loggingServiceFactory ) throws MetricsCollectionException
    {
        operationTypeMetricsManagers = toOperationTypeMetricsManagerArray(
                operationTypeToClassMapping,
                unit,
                highestExpectedRuntimeDurationAsNano,
                loggingServiceFactory
        );

        this.startTimeAsMilli = Long.MAX_VALUE;
        this.latestFinishTimeAsMilli = Long.MIN_VALUE;
        this.timeSource = timeSource;
        this.unit = unit;
    }

    final static long ONE_MS_AS_NS = TimeUnit.MILLISECONDS.toNanos( 1 );

    void measure( long actualStartTimeAsMilli, long runDurationAsNano, int operationType )
            throws MetricsCollectionException
    {
        if ( actualStartTimeAsMilli < startTimeAsMilli )
        {
            startTimeAsMilli = actualStartTimeAsMilli;
        }

        long operationFinishTimeAsMilli = actualStartTimeAsMilli + (runDurationAsNano / ONE_MS_AS_NS);
        if ( operationFinishTimeAsMilli > latestFinishTimeAsMilli )
        {
            latestFinishTimeAsMilli = operationFinishTimeAsMilli;
        }

        operationTypeMetricsManagers[operationType].measure( runDurationAsNano );
    }

    private long totalOperationCount()
    {
        long count = 0;
        for ( OperationTypeMetricsManager operationTypeMetricsManager : operationTypeMetricsManagers )
        {
            if ( null != operationTypeMetricsManager && operationTypeMetricsManager.count() > 0 )
            {
                count += operationTypeMetricsManager.count();
            }
        }
        return count;
    }

    WorkloadResultsSnapshot snapshot()
    {
        Map<String,OperationMetricsSnapshot> operationMetricsMap = new HashMap<>();
        for ( OperationTypeMetricsManager operationTypeMetricsManager : operationTypeMetricsManagers )
        {
            if ( null != operationTypeMetricsManager && operationTypeMetricsManager.count() > 0 )
            {
                OperationMetricsSnapshot snapshot = operationTypeMetricsManager.snapshot();
                operationMetricsMap.put( snapshot.name(), snapshot );
            }
        }
        return new WorkloadResultsSnapshot(
                operationMetricsMap,
                (startTimeAsMilli == Long.MAX_VALUE) ? -1 : startTimeAsMilli,
                (latestFinishTimeAsMilli == Long.MIN_VALUE) ? -1 : latestFinishTimeAsMilli,
                totalOperationCount(),
                unit );
    }

    WorkloadStatusSnapshot status()
    {
        long nowAsMilli = timeSource.nowAsMilli();
        if ( nowAsMilli < startTimeAsMilli )
        {
            long runDurationAsMilli = 0;
            long operationCount = 0;
            long durationSinceLastMeasurementAsMilli = 0;
            double operationsPerSecond = 0;
            return new WorkloadStatusSnapshot(
                    runDurationAsMilli,
                    operationCount,
                    durationSinceLastMeasurementAsMilli,
                    operationsPerSecond );
        }
        else
        {
            long runDurationAsMilli = nowAsMilli - startTimeAsMilli;
            long operationCount = totalOperationCount();
            long durationSinceLastMeasurementAsMilli =
                    (-1 == latestFinishTimeAsMilli) ? -1 : nowAsMilli - latestFinishTimeAsMilli;
            double operationsPerSecond =
                    ((double) operationCount / TimeUnit.MILLISECONDS.toNanos( runDurationAsMilli )) *
                    TimeUnit.SECONDS.toNanos( 1 );
            return new WorkloadStatusSnapshot(
                    runDurationAsMilli,
                    operationCount,
                    durationSinceLastMeasurementAsMilli,
                    operationsPerSecond );
        }
    }
}
