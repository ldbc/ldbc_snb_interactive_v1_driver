package com.ldbc.driver;

import com.ldbc.driver.util.ClassLoaderHelper;
import stormpot.Poolable;
import stormpot.Slot;

public class ReflectionOperationHandlerFactory implements OperationHandlerFactory {
    private static final Slot DUMMY_SLOT = new Slot() {
        @Override
        public void release(Poolable obj) {
            // do nothing
        }
    };
    private final Class<? extends OperationHandler> operationHandlerType;

    public ReflectionOperationHandlerFactory(Class<? extends OperationHandler> operationHandlerType) {
        this.operationHandlerType = operationHandlerType;
    }

    @Override
    public OperationHandler<?> newOperationHandler() throws OperationException {
        OperationHandler<?> operationHandler = ClassLoaderHelper.loadOperationHandler(operationHandlerType);
        operationHandler.setSlot(DUMMY_SLOT);
        return operationHandler;
    }

    @Override
    public void shutdown() {
        // nothing to do here
    }

    @Override
    public String toString() {
        return "ReflectionOperationHandlerFactory{" +
                "operationHandlerType=" + operationHandlerType +
                '}';
    }
}
