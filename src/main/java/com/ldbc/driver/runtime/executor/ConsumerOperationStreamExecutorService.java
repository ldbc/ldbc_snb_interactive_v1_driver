package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.String.format;

public class ConsumerOperationStreamExecutorService {
    public static final long SHUTDOWN_WAIT_TIMEOUT_AS_MILLI = TimeUnit.SECONDS.toMillis( 10 );

    private final ConsumerOperationStreamExecutorServiceThread operationStreamExecutorServiceThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean( false );
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean executing = new AtomicBoolean( false );
    private final AtomicBoolean shutdown = new AtomicBoolean( false );
    private final AtomicBoolean forceThreadToTerminate = new AtomicBoolean( false );

    public ConsumerOperationStreamExecutorService(
        ConcurrentErrorReporter errorReporter,
        OperationExecutor operationExecutor) {
        this.errorReporter = errorReporter;
        this.operationStreamExecutorServiceThread = new ConsumerOperationStreamExecutorServiceThread(
            operationExecutor,
            errorReporter,
            hasFinished,
            forceThreadToTerminate );
    }

    synchronized public AtomicBoolean execute() {
        if (executing.get()) {
            return hasFinished;
        }
        executing.set( true );
        operationStreamExecutorServiceThread.start();
        return hasFinished;
    }

    synchronized public void shutdown( long shutdownWait ) throws OperationExecutorException {
        if (shutdown.get()) {
            throw new OperationExecutorException( "Executor has already been shutdown" );
        }
        if (null != operationStreamExecutorServiceThread) {
            doShutdown( shutdownWait );
        }
        shutdown.set( true );
    }

    private void doShutdown( long shutdownWait ) {
        try {
            forceThreadToTerminate.set( true );
            operationStreamExecutorServiceThread.join( shutdownWait );
        } catch (Exception e) {
            String errMsg = format( "Unexpected error encountered while shutting down thread\n%s",
                ConcurrentErrorReporter.stackTraceToString( e ) );
            errorReporter.reportError( this, errMsg );
        }
    }
}