package org.ldbcouncil.snb.driver;

import org.ldbcouncil.snb.driver.temporal.TemporalUtil;
import stormpot.Allocator;
import stormpot.Pool;
import stormpot.Completion;
import stormpot.Expiration;
import stormpot.Slot;
import stormpot.SlotInfo;
import stormpot.Timeout;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class PoolingOperationHandlerRunnerFactory implements OperationHandlerRunnerFactory
{
    private static final int INITIAL_POOL_SIZE = 512;
    private static final int MAX_POOL_SIZE = (int) Math.round( Math.pow( 2, 15 ) ); // ~32,000
    private static final Timeout POOL_CLAIM_TIMEOUT = new Timeout( 100, TimeUnit.MILLISECONDS );
    private static final Timeout POOL_CLAIM_AFTER_RESIZE_TIMEOUT = new Timeout( 1000, TimeUnit.MILLISECONDS );
    private static final Timeout POOL_SHUTDOWN_TIMEOUT = new Timeout( 10, TimeUnit.SECONDS );
    private final Pool<OperationHandlerRunnableContext> operationHandlerRunnerPool;
    private final OperationHandlerRunnerFactory innerOperationHandlerRunnerFactory;
    int highestSetPoolSize = 0;

    public PoolingOperationHandlerRunnerFactory( OperationHandlerRunnerFactory operationHandlerRunnerFactory )
    {
        this.innerOperationHandlerRunnerFactory = operationHandlerRunnerFactory;

        OperationHandlerRunnerAllocator operationHandlerRunnerAllocator =
                new OperationHandlerRunnerAllocator( innerOperationHandlerRunnerFactory );
        this.operationHandlerRunnerPool = Pool.from(operationHandlerRunnerAllocator)
            .setBackgroundExpirationEnabled( false )
            .setPreciseLeakDetectionEnabled( false )
            .setExpiration(new NeverExpiration())
            .setSize(INITIAL_POOL_SIZE)
            .build();

        this.highestSetPoolSize = INITIAL_POOL_SIZE;
    }

    @Override
    public OperationHandlerRunnableContext newOperationHandlerRunner() throws OperationException
    {
        try
        {
            OperationHandlerRunnableContext operationHandlerRunner =
                    operationHandlerRunnerPool.claim( POOL_CLAIM_TIMEOUT );
            while ( null == operationHandlerRunner )
            {
                int currentPoolSize = operationHandlerRunnerPool.getTargetSize();
                if ( currentPoolSize < MAX_POOL_SIZE )
                {
                    operationHandlerRunnerPool.setTargetSize( currentPoolSize * 2 );
                    highestSetPoolSize = currentPoolSize * 2;
                }
                operationHandlerRunner = operationHandlerRunnerPool.claim( POOL_CLAIM_AFTER_RESIZE_TIMEOUT );
            }
            return operationHandlerRunner;
        }
        catch ( Exception e )
        {
            int currentPoolSize = operationHandlerRunnerPool.getTargetSize();
            throw new OperationException(
                    format( "Error encountered while attempting to allocate handler runner from pool\n"
                            + "Max pool size: %s\n"
                            + "Highest set pool size: %s\n"
                            + "Current pool size: %s",
                            MAX_POOL_SIZE,
                            highestSetPoolSize,
                            currentPoolSize ),
                    e
            );
        }
    }

    @Override
    public void shutdown() throws OperationException
    {
        TemporalUtil temporalUtil = new TemporalUtil();
        innerOperationHandlerRunnerFactory.shutdown();
        Completion completion = operationHandlerRunnerPool.shutdown();
        try
        {
            boolean isSuccessfulShutdown = completion.await( POOL_SHUTDOWN_TIMEOUT );
            if (!isSuccessfulShutdown )
            {
                throw new OperationException(
                        format(
                                "Operation handler pool did not shutdown before timeout: %s\n"
                                + "Pool Target Size: %s\n"
                                + "Pool Allocation Count: %s\n"
                                + "Pool Failed Allocation Count: %s\n"
                                + "Pool Leaked Objects Count: %s\n",
                                temporalUtil.milliDurationToString( TimeUnit.MILLISECONDS
                                        .convert( POOL_SHUTDOWN_TIMEOUT.getTimeout(),
                                                POOL_SHUTDOWN_TIMEOUT.getUnit() ) ),
                                operationHandlerRunnerPool.getTargetSize(),
                                operationHandlerRunnerPool.getManagedPool().getAllocationCount(),
                                operationHandlerRunnerPool.getManagedPool().getFailedAllocationCount(),
                                operationHandlerRunnerPool.getManagedPool().getLeakedObjectsCount()

                        )
                );
            }
        }
        catch ( InterruptedException e )
        {
            throw new OperationException( "Error encountered while shutting down operation handler pool", e );
        }
    }

    @Override
    public String toString()
    {
        return PoolingOperationHandlerRunnerFactory.class.getSimpleName() + "{" +
               innerOperationHandlerRunnerFactory.toString() + "}";
    }

    private static class OperationHandlerRunnerAllocator implements Allocator<OperationHandlerRunnableContext>
    {
        private final OperationHandlerRunnerFactory operationHandlerRunnerFactory;

        public OperationHandlerRunnerAllocator( OperationHandlerRunnerFactory operationHandlerRunnerFactory )
        {
            this.operationHandlerRunnerFactory = operationHandlerRunnerFactory;
        }

        @Override
        public OperationHandlerRunnableContext allocate( Slot slot ) throws Exception
        {
            OperationHandlerRunnableContext operationHandlerRunner =
                    operationHandlerRunnerFactory.newOperationHandlerRunner();
            operationHandlerRunner.setSlot( slot );
            return operationHandlerRunner;
        }

        @Override
        public void deallocate( OperationHandlerRunnableContext operationHandlerRunner ) throws Exception
        {
            // I think nothing needs to be done here
        }
    }

    private static class NeverExpiration implements Expiration<OperationHandlerRunnableContext>
    {
        @Override
        public boolean hasExpired( SlotInfo<? extends OperationHandlerRunnableContext> slotInfo ) throws Exception
        {
            return false;
        }
    }
}
