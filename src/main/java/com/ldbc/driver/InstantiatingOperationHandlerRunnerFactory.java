package com.ldbc.driver;

import stormpot.Poolable;
import stormpot.Slot;

public class InstantiatingOperationHandlerRunnerFactory implements OperationHandlerRunnerFactory
{
    private static final Slot DUMMY_SLOT = new Slot()
    {
        @Override
        public void release( Poolable obj )
        {
            // do nothing
        }

        @Override
        public void expire( Poolable poolable )
        {
            // do nothing
        }
    };

    @Override
    public OperationHandlerRunnableContext newOperationHandlerRunner() throws OperationException
    {
        OperationHandlerRunnableContext operationHandlerRunnableContext = new OperationHandlerRunnableContext();
        operationHandlerRunnableContext.setSlot( DUMMY_SLOT );
        return operationHandlerRunnableContext;
    }

    @Override
    public void shutdown() throws OperationException
    {
        // nothing to do here
    }
}
