package com.ldbc.driver;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.DummyCountingMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import org.junit.Test;

import static java.lang.String.format;

public class OperationHandlerRunnableContextFactoryTest
{
    @Test
    public void shouldRunOperationHandlerTest() throws OperationException, InterruptedException
    {
        Operation operation = new NothingOperation();
        int count = 100;
        while ( count < 10000000 )
        {
            OperationHandlerRunnerFactory instantiatingOperationHandlerRunnerFactory =
                    new InstantiatingOperationHandlerRunnerFactory();
            OperationHandlerRunnerFactory pooledInstantiatingOperationHandlerRunnerFactory =
                    new PoolingOperationHandlerRunnerFactory( new InstantiatingOperationHandlerRunnerFactory() );
            long instantiatingDuration =
                    doOperationHandlerTest( count, instantiatingOperationHandlerRunnerFactory, operation );
            long pooledInstantiatingDuration =
                    doOperationHandlerTest( count, pooledInstantiatingOperationHandlerRunnerFactory, operation );
            count = count * 4;
            System.out.println( format( "Count: %s, Instantiating: %s, PooledInstantiating: %s, Speedup: %s", count,
                    instantiatingDuration, pooledInstantiatingDuration,
                    instantiatingDuration / (double) pooledInstantiatingDuration ) );
            instantiatingOperationHandlerRunnerFactory.shutdown();
            pooledInstantiatingOperationHandlerRunnerFactory.shutdown();
        }
    }

    public long doOperationHandlerTest( int count, OperationHandlerRunnerFactory operationHandlerRunnerFactory,
            Operation operation ) throws OperationException
    {
        boolean ignoreScheduledStartTime = false;
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        long spinnerSleepDuration = 0;
        Spinner spinner = new Spinner( timeSource, spinnerSleepDuration, ignoreScheduledStartTime );
        LocalCompletionTimeWriter localCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        MetricsService metricsService = new DummyCountingMetricsService();
        long startTime = timeSource.nowAsMilli();
        for ( int i = 0; i < count; i++ )
        {
            OperationHandlerRunnableContext operationHandler =
                    operationHandlerRunnerFactory.newOperationHandlerRunner();
            operationHandler
                    .init( timeSource, spinner, operation, localCompletionTimeWriter, errorReporter, metricsService );
            operationHandler.cleanup();
        }
        return timeSource.nowAsMilli() - startTime;
    }
}
